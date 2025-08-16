
# database.py - Database configuration and models
from sqlalchemy import create_engine, Column, String, Integer, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
import os

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/cities_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class City(Base):
    """
    SQLAlchemy model for cities table
    Stores city names with their country codes and timestamps
    """
    __tablename__ = "cities"
    
    id = Column(Integer, primary_key=True, index=True)
    city = Column(String, unique=True, index=True, nullable=False)
    country_code = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

def create_tables():
    """Create all database tables"""
    Base.metadata.create_all(bind=engine)

def get_db() -> Session:
    """
    Dependency function to get database session
    Ensures proper session cleanup after each request
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
