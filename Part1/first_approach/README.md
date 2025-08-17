# City Country Code API - Complete Setup Guide

This guide walks you through setting up a complete Data Eng application with FastAPI, PostgreSQL, Redis, and Kafka.

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

Create/Clone the project directory and files:

```
project/
│
├── docker-compose.yml
├── main.py
├── models.py
├── load_data.py
├── update_city_test.py
├── README.md
└── requirements.txt
```

### Step 2: Install Python Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  

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

```

**Wait 30-60 seconds** for Kafka to fully initialize before proceeding.

### Step 4: Initialize Kafka Topic

Create the required Kafka topic for logging inside the Kafka Container:

```bash
$ docker exec -it kafka_broker bash
$ kafka-topics.sh --create \
  --topic city_requests \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
$ exit
```

### Step 5: Start the FastAPI Application
Run one of these options (Do not shoot it down for the next steps) 
```bash
# Start the API server
python main.py

# Or use uvicorn directly:
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at: http://localhost:8000
- API documentation: http://localhost:8000/docs 

### Step 6: Load City Data

Load initial city data into the database:

```bash
# This will load sample cities
python load_data.py
```

Expected output:
```
API is ready!
Processing file: CountryCode-City.csv
Inserted 100 cities...
...
Inserted 10000 cities...

Data loading complete!
Total cities processed:10000
Successfully inserted: 10000
Failed: 0

```

## API Endpoints Explanation

### 1. Create/Update City
```http
POST /city
Content-Type: application/json

{
  "city": "London",
  "country_code": "GB"
}
```


### 2. Get City Country Code
```http
GET /cities/{city_name}
```
### 3. Check Connection Health
```http
GET /health
```
### 4. Root
```http
GET /
```
### 5. Statistics
```http
GET /stats
```

## View Kafka Logs:

``` bash
docker exec -it kafka_broker bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic city_requests \
  --from-beginning
```
