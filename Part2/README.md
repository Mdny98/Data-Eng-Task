# SMS Transaction Analytics with Apache Spark Streaming

A real-time data analytics solution using Apache Spark Structured Streaming to process SMS transaction data and generate comprehensive reports stored in MinIO.



### some notes:

**Apache Spark Structured Streaming:**
Micro-batch Processing
Incremental Processing : (trigger)
Unified API
Automatic Optimization: (Catalyst optimizer)


Key Concepts:
- Watermarking: Handles late-arriving data (wait time)
- Windowing: Groups data into time-based windows (like our 15-minute intervals)
- Checkpointing: Saves processing state to recover from failures
- Output Modes: Complete, Append, or Update 

### MinIO Object Storage:
MinIO is a high-performance, S3-compatible object storage system designed for cloud-native applications and distributed systems.
S3 Compatibility: Uses the same API as Amazon S3, making it interoperable with many tools (means a storage system uses the Amazon S3 API (Simple Storage Service API) to interact with applications and other systems) 
Distributed Architecture: Can scale across multiple servers and disks
Erasure Coding: Provides data protection and redundancy without full replication
RESTful API: Simple HTTP-based API for storing and retrieving objects

## Features

- **Real-time Processing**: Continuous data processing using Spark Structured Streaming
- **Multiple Analytics Reports**: Four different analytical reports with time-based aggregations
- **MinIO Integration**: All outputs stored as CSV files in MinIO object storage
- **Reference Data Mapping**: PayType transformation using Excel reference file
- **Fault Tolerance**: Built-in checkpointing and error handling

## Quick Start

### Prerequisites
- Apache Spark 3.x
- Python 3.7+
- MinIO Server
- Java 8/11

### Installation
```bash
# Install Python dependencies
pip install pyspark pandas openpyxl

# Start MinIO server
./minio server ~/minio-data --console-address ":9001"
```

### Data Setup
Place your data files in the `REF_SMS/` directory:
```
REF_SMS/
├── REF_CBS_SMS2.csv    # Main transaction data
└── Ref.xlsx           # PayType reference (PayType, value columns)
```

### Run Analytics
```bash
# Method 1: Direct execution
python spark_streaming_analytics.py

# Method 2: Using spark-submit (recommended)
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  spark_streaming_analytics.py
```

## Generated Reports

| Report | Description | Output Location |
|--------|-------------|-----------------|
| **Daily Revenue** | Total daily revenue in Toman | `analytics-output/daily_revenue/` |
| **15-Min Revenue by PayType** | Revenue per 15-min intervals by payment type | `analytics-output/15min_revenue_by_paytype/` |
| **Max/Min Revenue** | Peak and minimum revenue analysis | `analytics-output/max_min_revenue/` |
| **Revenue & Count with Mapping** | Detailed analysis with meaningful PayType names | `analytics-output/revenue_count_with_mapping/` |

## Data Format

### Input CSV Format
```csv
RECORD_DATE,PayType,Debit_amount_42
12:40:34 2021/06/22,0,25000000
12:41:15 2021/06/22,1,15000000
```

### Reference Excel Format
```
PayType | value
0       | Prepaid
1       | Postpaid
2       | Corporate
```

## Configuration

Update MinIO connection settings in the script:
```python
analytics = SparkStreamingAnalytics(
    minio_endpoint="localhost:9000",
    minio_access_key="your-access-key",
    minio_secret_key="your-secret-key"
)
```

## Monitoring

- **Spark UI**: `http://localhost:4040` - Monitor streaming progress
- **MinIO Console**: `http://localhost:9001` - View generated CSV files
- **Logs**: Check terminal output for processing statistics

## Architecture

```
CSV Files → Spark Streaming → Data Processing → MinIO Storage
    ↓              ↓               ↓              ↓
REF_SMS/    Time Windows    Aggregations    CSV Reports
```

## Key Transformations

1. **Revenue Calculation**: `Debit_amount_42 ÷ 1000` (milli-rials to Toman)
2. **Time Processing**: Converts "HH:mm:ss yyyy/MM/dd" to timestamp
3. **15-Minute Windows**: Tumbling windows for interval analysis
4. **PayType Mapping**: Maps codes to meaningful names using reference file

## Stopping the Application

- **Graceful**: Press `Ctrl+C`
- **Force**: Kill the process
- **Programmatic**: Call `analytics.stop_all_queries()`

## Troubleshooting

| Issue | Solution |
|-------|----------|
| MinIO connection fails | Verify MinIO server is running and credentials are correct |
| Schema mismatch | Check CSV file format and column names |
| Out of memory | Increase Spark driver/executor memory settings |
| Checkpoint errors | Clear checkpoint directories when changing query logic |

## Project Structure

```
├── spark_streaming_analytics.py    # Main application
├── REF_SMS/                        # Input data directory
│   ├── REF_CBS_SMS2.csv           # Transaction data
│   └── Ref.xlsx                   # Reference mapping
└── README.md                      # This file
```

## Sample Output

### Daily Revenue Report
```csv
date,total_revenue
2021-06-22,1250000
2021-06-23,1180000
```

### 15-Minute Revenue with Mapping
```csv
RECORD_DATE,Pay_Type,Record_Count,Revenue
2021-06-22 12:00:00,Prepaid,20,25000
2021-06-22 12:00:00,Postpaid,30,15000
```

## Performance Notes

- Processing trigger set to 30 seconds
- Watermark of 1 minute for late data
- Optimized for real-time analytics workloads
- Scales horizontally with additional Spark workers

## License

This project is part of a data engineering coursework solution.
