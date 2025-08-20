# City Country Code API

A high-performance data engineering application that provides city-to-country code mapping with caching and request logging capabilities.

## System Architecture

The application consists of:
- **FastAPI**: REST API server for city/country code operations
- **PostgreSQL**: Primary database for persistent storage
- **Redis**: LRU cache (max 10 items, 10-minute TTL)
- **Apache Kafka**: Request logging and analytics
- **Docker**: Service containerization and orchestration

## Core Features

**Data Management**
- Create/update city records with country codes
- Retrieve country codes by city name
- Automatic cache management with LRU eviction

**Performance Optimization**  
- Redis caching for frequently requested cities
- Cache hit/miss tracking and statistics
- Response time monitoring

**Observability**
- All requests logged to Kafka with metadata
- Cache hit percentage tracking
- Health monitoring endpoints


## Prerequisites

Make sure you have installed:
- Python 3.8+
- Docker and Docker Compose
- pip (Python package manager)

## Quick Start

### Project Structure Setup

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

### Start Infrastructure Services

Start PostgreSQL, Redis, and Kafka using Docker Compose:

```bash
# Start all services in background
docker-compose up -d
```
‍*‍*‍Wait 30-60 seconds** for Kafka to fully initialize before proceeding.

### Initialize Kafka Topic

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

### Start the FastAPI Application
Run one of these options (Do not shoot it down for the next steps) 
```bash
# Start the API server
python main.py

# Or use uvicorn directly:
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at: http://localhost:8000
- API documentation: http://localhost:8000/docs 

### Load City Data

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
### 5. View Statistics
```http
GET /stats
```

Access interactive API documentation at `http://localhost:8000/docs`

## Cache Behavior

- **Capacity**: 10 most recent requests
- **TTL**: 10 minutes per entry
- **Strategy**: LRU (Least Recently Used) eviction
- **Performance**: Sub-millisecond response times for cached data


## Monitoring
View real-time request logs via Kafka:

``` bash
docker exec -it kafka_broker bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic city_requests \
  --from-beginning
```
Each log entry includes response time, cache status, and hit rate statistics.
