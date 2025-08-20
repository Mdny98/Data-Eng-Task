#!/usr/bin/env python3
"""
Quick setup and troubleshooting script for Spark Streaming Analytics
Run this first to verify your environment setup
"""

import sys
import os

def check_environment():
    """Check if all required components are available"""
    print("=== Environment Check ===")
    
    # Check Python version
    print(f"Python version: {sys.version}")
    
    # Check required packages
    packages = ['pyspark', 'pandas', 'openpyxl']
    for package in packages:
        try:
            __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            print(f"✗ {package} is NOT installed. Run: pip install {package}")
    
    # Check Java
    java_home = os.environ.get('JAVA_HOME')
    if java_home:
        print(f"✓ JAVA_HOME: {java_home}")
    else:
        print("⚠ JAVA_HOME not set. Spark may still work with system Java")
    
    # Check Spark
    spark_home = os.environ.get('SPARK_HOME')
    if spark_home:
        print(f"✓ SPARK_HOME: {spark_home}")
    else:
        print("⚠ SPARK_HOME not set. Using PySpark from pip installation")

def create_sample_data():
    """Create sample data files for testing"""
    print("\n=== Creating Sample Data ===")
    
    # Create REF_SMS directory
    os.makedirs("REF_SMS", exist_ok=True)
    
    # Create sample CSV data
    csv_data = """RECORD_DATE,PayType,Debit_amount_42
12:40:34 2021/06/22,0,25000000
12:41:15 2021/06/22,1,15000000
12:42:30 2021/06/22,0,30000000
12:43:45 2021/06/22,2,20000000
12:55:10 2021/06/22,1,18000000
12:56:25 2021/06/22,0,22000000
13:01:15 2021/06/22,1,16000000
13:02:30 2021/06/22,0,28000000"""
    
    with open("REF_SMS/REF_CBS_SMS2.csv", "w") as f:
        f.write(csv_data)
    print("✓ Created sample CSV file: REF_SMS/REF_CBS_SMS2.csv")
    
    # Create sample Excel reference file using pandas
    try:
        import pandas as pd
        ref_data = {
            'PayType': [0, 1, 2],
            'value': ['Prepaid', 'Postpaid', 'Corporate']
        }
        ref_df = pd.DataFrame(ref_data)
        ref_df.to_excel("REF_SMS/Ref.xlsx", index=False)
        print("✓ Created reference Excel file: REF_SMS/Ref.xlsx")
    except Exception as e:
        print(f"✗ Could not create Excel file: {e}")
        # Create CSV fallback
        with open("REF_SMS/Ref.csv", "w") as f:
            f.write("PayType,value\n0,Prepaid\n1,Postpaid\n2,Corporate")
        print("✓ Created reference CSV file: REF_SMS/Ref.csv (fallback)")

def test_basic_spark():
    """Test basic Spark functionality"""
    print("\n=== Testing Basic Spark ===")
    
    try:
        from pyspark.sql import SparkSession
        
        # Create a minimal Spark session
        spark = SparkSession.builder \
            .appName("Test") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Test basic operation
        test_data = [(1, "test"), (2, "data")]
        df = spark.createDataFrame(test_data, ["id", "value"])
        count = df.count()
        
        print(f"✓ Spark is working! Test DataFrame has {count} rows")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"✗ Spark test failed: {e}")
        return False

def simplified_main():
    """Simplified version of the main analytics without MinIO"""
    print("\n=== Running Simplified Analytics ===")
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, to_timestamp, to_date, sum
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

        # Create Spark session
        spark = SparkSession.builder \
            .appName("SMS_Analytics_Simple") \
            .master("local[*]") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        # Read the sample data as batch (not streaming) for testing
        sms_schema = StructType([
            StructField("RECORD_DATE", StringType(), True),
            StructField("PayType", IntegerType(), True),
            StructField("Debit_amount_42", LongType(), True),
        ])
        
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(sms_schema) \
            .load("REF_SMS/REF_CBS_SMS2.csv")
        
        # Transform data
        processed_df = df \
            .withColumn("timestamp", to_timestamp(col("RECORD_DATE"), "HH:mm:ss yyyy/MM/dd")) \
            .withColumn("revenue_toman", col("Debit_amount_42") / 1000) \
            .filter(col("timestamp").isNotNull())
        
        print("✓ Data loaded and processed successfully!")
        print("Sample data:")
        processed_df.show(5)
        
        # Simple daily revenue calculation
        daily_revenue = processed_df \
            .withColumn("date", to_date(col("timestamp"))) \
            .groupBy("date") \
            .agg(sum("revenue_toman").alias("total_revenue"))
        
        print("Daily Revenue:")
        daily_revenue.show()
        
        # Save to local CSV
        daily_revenue.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("output/daily_revenue_test")
        
        print("✓ Test output saved to: output/daily_revenue_test/")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"✗ Simplified analytics failed: {e}")
        return False

if __name__ == "__main__":
    print("SMS Analytics - Environment Setup and Test\n")
    
    # Step 1: Check environment
    check_environment()
    
    # Step 2: Create sample data
    create_sample_data()
    
    # Step 3: Test basic Spark
    if test_basic_spark():
        print("\n=== Basic Spark test passed! ===")
        
        # Step 4: Run simplified analytics
        if simplified_main():
            print("\n🎉 SUCCESS! Your environment is ready.")
            print("\nNext steps:")
            print("1. Check the 'output' directory for generated files")
            print("2. Run the full streaming analytics with: python main.py")
        else:
            print("\n❌ Simplified analytics failed. Check error messages above.")
    else:
        print("\n❌ Basic Spark test failed. Please fix Spark installation first.")
        print("\nTroubleshooting tips:")
        print("1. Make sure Java 8 or 11 is installed")
        print("2. Try: pip install --upgrade pyspark")
        print("3. Set JAVA_HOME environment variable")

