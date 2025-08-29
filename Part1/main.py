import time
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, City
import redis
from kafka import KafkaProducer
import json

# Environment variables with defaults
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/cities_db")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Database connection
engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=40,
    pool_timeout=30,
    pool_recycle=1800
)
SessionLocal = sessionmaker(bind=engine)

# Create tables
Base.metadata.create_all(bind=engine)

# Redis connection with retry logic
def create_redis_client():
    max_retries = 5
    for i in range(max_retries):
        try:
            client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            client.ping()  # Test connection
            return client
        except redis.ConnectionError:
            if i < max_retries - 1:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                raise

redis_client = create_redis_client()

# Kafka producer with retry logic
def create_kafka_producer():
    max_retries = 5
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                retries=5,
                request_timeout_ms=30000,
                max_block_ms=30000,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            return producer
        except Exception as e:
            print(f"Failed to create Kafka producer (attempt {i+1}): {e}")
            if i < max_retries - 1:
                time.sleep(2 ** i)  # Exponential backoff
            else:
                raise

producer = create_kafka_producer()
KAFKA_TOPIC = "city_requests"

# Cache stats
total_requests = 0
cache_hits = 0

# FastAPI app
app = FastAPI(title="Cities API", version="1.0.0")

class CitySchema(BaseModel):
    name: str
    country_code: str

@app.get("/")
def root():
    return {"message": "Cities API is running"}

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "database": "connected",
        "redis": "connected",
        "kafka": "connected"
    }

@app.post("/city")
def create_or_update_city(city: CitySchema):
    with SessionLocal() as db:
        db_city = db.query(City).filter(City.name == city.name).first()
        if db_city:
            db_city.country_code = city.country_code
            # Cache Coherency (update redis if it exists)
            if redis_client.exists(city.name):
                redis_client.setex(city.name, 600, city.country_code)
                # Update access time
                redis_client.zadd("cache_access_times", {city.name: time.time()})
        else:
            db_city = City(name=city.name, country_code=city.country_code)
            db.add(db_city)
        db.commit()
        db.refresh(db_city)
    return {"message": "City added/updated successfully"}

@app.get("/city/{city_name}")
def get_country_code(city_name: str):
    global total_requests, cache_hits
    start_time = time.time()
    total_requests += 1
    
    # Check Redis
    country_code = redis_client.get(city_name)
    if country_code:
        cache_hits += 1
        cache_status = "HIT"
        # Update access time for LRU
        redis_client.zadd("cache_access_times", {city_name: time.time()})
    else:
        with SessionLocal() as db:
            db_city = db.query(City).filter(City.name == city_name).first()
            if not db_city:
                raise HTTPException(status_code=404, detail="City not found")
            country_code = db_city.country_code
            
            # Before adding new item, check cache size (For Holding 10th recent requests)
            cache_size = redis_client.zcard("cache_access_times")
            if cache_size >= 10:
                # Remove oldest item
                oldest_items = redis_client.zrange("cache_access_times", 0, 0)
                if oldest_items:
                    oldest_city = oldest_items[0]
                    redis_client.delete(oldest_city)
                    redis_client.zrem("cache_access_times", oldest_city)
            redis_client.setex(city_name, 600, country_code)  # expire in 10 min
            redis_client.zadd("cache_access_times", {city_name: time.time()})
            cache_status = "MISS"
    
    # Publish Kafka log
    response_time = time.time() - start_time
    try:
        producer.send(KAFKA_TOPIC, {
            "city": city_name,
            "response_time": response_time,
            "cache_status": cache_status,
            "cache_hit_percentage": round((cache_hits / total_requests) * 100, 2)
        })
    except Exception as e:
        print(f"Failed to send Kafka message: {e}")
    
    return {
        "city": city_name,
        "country_code": country_code,
        "cache_status": cache_status,
        "response_time": time.time() - start_time
    }

@app.get("/stats")
def get_stats():
    return {
        "total_requests": total_requests,
        "cache_hits": cache_hits,
        "cache_hit_percentage": round((cache_hits / total_requests) * 100, 2) if total_requests > 0 else 0
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


# Check Kafka Producer

# docker exec -it kafka_broker bash

# kafka-console-consumer.sh \
#   --bootstrap-server localhost:9092 \
#   --topic city_requests \
#   --from-beginning