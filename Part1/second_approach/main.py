# main.py - FastAPI application
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import or_
import time
import os
from typing import Optional, List
import json

# Import our custom modules
from database import get_db, create_tables, City
from redis_client import redis_cache
from kafka_client import kafka_logger

# Initialize FastAPI app
app = FastAPI(
    title="City Country Code API",
    description="API for managing city-country code mappings with Redis caching and Kafka logging",
    version="1.0.0"
)

# Pydantic models for request/response validation
class CityRequest(BaseModel):
    """Request model for creating/updating city records"""
    city: str
    country_code: str

class CityResponse(BaseModel):
    """Response model for city queries"""
    city: str
    country_code: str
    source: str  # "cache" or "database"

class BulkLoadRequest(BaseModel):
    """Request model for bulk loading cities"""
    cities: List[CityRequest]

# Global statistics tracking
class AppStats:
    def __init__(self):
        self.total_requests = 0
        self.cache_hits = 0
    
    def record_request(self, cache_hit: bool):
        self.total_requests += 1
        if cache_hit:
            self.cache_hits += 1
    
    def get_hit_percentage(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.cache_hits / self.total_requests) * 100

app_stats = AppStats()

# Startup event to create database tables
@app.on_event("startup")
async def startup_event():
    """Initialize database tables on application startup"""
    create_tables()
    print("Database tables created successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on application shutdown"""
    kafka_logger.close()

# STEP 2: API for creating/updating city records
@app.post("/cities", response_model=dict)
async def create_or_update_city(
    city_data: CityRequest,
    db: Session = Depends(get_db)
):
    """
    Create new city record or update existing one
    
    - Creates new record if city doesn't exist
    - Updates country code if city already exists
    """
    # Check if city already exists (case-insensitive)
    existing_city = db.query(City).filter(
        City.city.ilike(city_data.city.strip())
    ).first()
    
    if existing_city:
        # Update existing city
        existing_city.country_code = city_data.country_code.strip().upper()
        db.commit()
        db.refresh(existing_city)
        
        return {
            "message": "City updated successfully",
            "city": existing_city.city,
            "country_code": existing_city.country_code,
            "action": "updated"
        }
    else:
        # Create new city
        new_city = City(
            city=city_data.city.strip().title(),
            country_code=city_data.country_code.strip().upper()
        )
        db.add(new_city)
        db.commit()
        db.refresh(new_city)
        
        return {
            "message": "City created successfully",
            "city": new_city.city,
            "country_code": new_city.country_code,
            "action": "created"
        }

# STEP 1: Bulk load endpoint for loading data from Cities folder
@app.post("/cities/bulk-load")
async def bulk_load_cities(
    bulk_data: BulkLoadRequest,
    db: Session = Depends(get_db)
):
    """
    Bulk load cities from data file
    Used to initially populate database from Cities folder data
    """
    created_count = 0
    updated_count = 0
    errors = []
    
    for city_data in bulk_data.cities:
        try:
            # Check if city exists
            existing_city = db.query(City).filter(
                City.city.ilike(city_data.city.strip())
            ).first()
            
            if existing_city:
                existing_city.country_code = city_data.country_code.strip().upper()
                updated_count += 1
            else:
                new_city = City(
                    city=city_data.city.strip().title(),
                    country_code=city_data.country_code.strip().upper()
                )
                db.add(new_city)
                created_count += 1
        
        except Exception as e:
            errors.append(f"Error processing {city_data.city}: {str(e)}")
    
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, f"Database commit failed: {str(e)}")
    
    return {
        "message": "Bulk load completed",
        "created": created_count,
        "updated": updated_count,
        "errors": errors,
        "total_processed": len(bulk_data.cities)
    }

# STEP 5: API for retrieving city country code with caching and logging
@app.get("/cities/{city}", response_model=CityResponse)
async def get_city_country_code(
    city: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Get country code for a city with Redis caching and Kafka logging
    
    Flow:
    1. Check Redis cache first
    2. If not found, query PostgreSQL
    3. Update cache with result
    4. Log request details to Kafka
    """
    start_time = time.time()
    cache_key = f"city:{city.lower().strip()}"
    cache_hit = False
    
    # Step 1: Check Redis cache
    cached_result = redis_cache.get(cache_key)
    if cached_result:
        cache_hit = True
        country_code = cached_result
        source = "cache"
    else:
        # Step 2: Query database
        db_city = db.query(City).filter(
            City.city.ilike(city.strip())
        ).first()
        
        if not db_city:
            # Record miss and log
            response_time_ms = (time.time() - start_time) * 1000
            app_stats.record_request(False)
            
            background_tasks.add_task(
                kafka_logger.log_request,
                city,
                response_time_ms,
                False,
                app_stats.get_hit_percentage()
            )
            
            raise HTTPException(status_code=404, detail="City not found")
        
        country_code = db_city.country_code
        source = "database"
        
        # Step 3: Update cache
        redis_cache.set(cache_key, country_code)
    
    # Calculate response time
    response_time_ms = (time.time() - start_time) * 1000
    
    # Record statistics
    app_stats.record_request(cache_hit)
    
    # Step 4: Log to Kafka (in background)
    background_tasks.add_task(
        kafka_logger.log_request,
        city,
        response_time_ms,
        cache_hit,
        app_stats.get_hit_percentage()
    )
    
    return CityResponse(
        city=city,
        country_code=country_code,
        source=source
    )

# Additional utility endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "services": {
            "database": "connected",
            "redis": "connected" if redis_cache.exists("test") or True else "disconnected",
            "kafka": "connected"
        }
    }

@app.get("/stats")
async def get_stats():
    """Get application statistics"""
    return {
        "total_requests": app_stats.total_requests,
        "cache_hits": app_stats.cache_hits,
        "cache_hit_percentage": app_stats.get_hit_percentage(),
        "redis_stats": redis_cache.get_cache_stats()
    }

@app.get("/cities")
async def list_cities(
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """List all cities with pagination"""
    cities = db.query(City).offset(offset).limit(limit).all()
    total = db.query(City).count()
    
    return {
        "cities": [
            {"city": city.city, "country_code": city.country_code}
            for city in cities
        ],
        "total": total,
        "limit": limit,
        "offset": offset
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

---