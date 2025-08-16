import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, City
import redis
from kafka import KafkaProducer
import json
import os

# Database connection
DATABASE_URL = "postgresql://user:password@localhost:5432/cities_db"

# engine = create_engine(DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

engine = create_engine(
    DATABASE_URL,
    pool_size=20,           # more connections
    max_overflow=40,        # extra overflow connections
    pool_timeout=30,        # wait before failing
    pool_recycle=1800       # recycle connections every 30 mins
)
SessionLocal = sessionmaker(bind=engine)


# Create tables
Base.metadata.create_all(bind=engine)

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Kafka producer
# producer = KafkaProducer(
#     bootstrap_servers=['localhost:9092'],
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],   # match docker service name
    retries=5,
    request_timeout_ms=20000,
    max_block_ms=20000,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
KAFKA_TOPIC = "city_requests"

# Cache stats
total_requests = 0
cache_hits = 0

# FastAPI app
app = FastAPI()

class CitySchema(BaseModel):
    name: str
    country_code: str

@app.post("/city")
def create_or_update_city(city: CitySchema):
    with SessionLocal() as db:
        db_city = db.query(City).filter(City.name == city.name).first()
        if db_city:
            db_city.country_code = city.country_code
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
    else:
        db = SessionLocal()
        db_city = db.query(City).filter(City.name == city_name).first()
        # print("#### Country code : ",db_city.country_code)
        # print("#### name : ",db_city.name)
        if not db_city:
            raise HTTPException(status_code=404, detail="City not found")
        country_code = db_city.country_code
        redis_client.setex(city_name, 600, country_code)  # expire in 10 min
        cache_status = "MISS"

    # Publish Kafka log
    response_time = time.time() - start_time
    producer.send(KAFKA_TOPIC, {
        "city": city_name,
        "response_time": response_time,
        "cache_status": cache_status,
        "cache_hit_percentage": round((cache_hits / total_requests) * 100, 2)
    })

    return {"city": city_name, "country_code": country_code}
