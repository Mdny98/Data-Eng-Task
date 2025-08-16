# City Country Code API - Complete Setup Guide

This guide walks you through setting up a complete Data Engineering application with FastAPI, PostgreSQL, Redis, and Kafka.

## Architecture Overview

The application consists of:
- **FastAPI**: REST API for city/country code management
- **PostgreSQL**: Primary database for persistent storage
- **Redis**: LRU cache (max 10 items, 10-minute TTL)
- **Apache Kafka**: Event logging for all API requests
- **Docker**: Containerization for all services

## Prerequisites

Make sure you have installed:
- Python 3.8+
- Docker and Docker Compose
- pip (Python package manager)

## Step-by-Step Setup

### Step 1: Project Structure Setup

Create your project directory and files:

```bash
mkdir city-api-project
cd city-api-project

# Create all the Python files from the code artifact above:
# - requirements.txt
# - docker-compose.yml  
# - database.py
# - redis_client.py
# - kafka_client.py
# - main.py
# - data_loader.py
# - kafka_setup.py
# - test_api.py

# Optional: Create Cities folder with sample data
mkdir Cities
```

### Step 2: Install Python Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Start Infrastructure Services

Start PostgreSQL, Redis, and Kafka using Docker Compose:

```bash
# Start all services in background
docker-compose up -d

# Check if services are running
docker-compose ps

# View logs if needed
docker-compose logs postgres
docker-compose logs redis
docker-compose logs kafka
```

**Wait 30-60 seconds** for Kafka to fully initialize before proceeding.

### Step 4: Initialize Kafka Topic

Create the required Kafka topic for logging:

```bash
python kafka_setup.py
```

Expected output:
```
Waiting for Kafka to be ready...
Kafka is ready!
Topic 'city-logs' created successfully
```

### Step 5: Start the FastAPI Application

```bash
# Start the API server
python main.py

# Or use uvicorn directly:
# uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at: http://localhost:8000
- API documentation: http://localhost:8000/docs
- Alternative docs: http://localhost:8000/redoc

### Step 6: Load Sample Data

Load initial city data into the database:

```bash
# This will load sample cities if no Cities folder exists
python data_loader.py
```

Expected output:
```
Cities folder not found. Creating sample data...
Bulk load successful:
  Created: 10
  Updated: 0
  Total processed: 10
```

### Step 7: Test the API

Run the comprehensive test suite:

```bash
python test_api.py
```

Expected output showing cache hits/misses and successful operations.

## API Endpoints Explanation

### 1. Create/Update City (Step 2)
```http
POST /cities
Content-Type: application/json

{
  "city": "London",
  "country_code": "GB"
}
```
- Creates new city or updates existing one
- Stores in PostgreSQL database

### 2. Bulk Load Cities (Step 1)
```http
POST /cities/bulk-load
Content-Type: application/json

{
  "cities": [
    {"city": "Paris", "country_code": "FR"},
    {"city": "Berlin", "country_code": "DE"}
  ]
}
```
- Used for initial data loading from Cities folder

### 3. Get City Country Code (Steps 3, 4, 5)
```http
GET /cities/{city_name}
```
**This endpoint implements the complete caching and logging flow:**

1. **Check Redis Cache**: Searches for city in Redis