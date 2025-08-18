
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

class SparkStreamingAnalytics:
    def __init__(self, minio_endpoint="localhost:9000", minio_access_key="minioadmin", minio_secret_key="minioadmin"):
        
        # Initialize Spark Session with MinIO configuration

        self.spark = SparkSession.builder \
            .appName("SMS_Analytics_Streaming") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}") \
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # MinIO bucket configuration
        self.output_bucket = "s3a://analytics-output"
        
    def load_reference_data(self, ref_file_path="REF_SMS/Ref.xlsx"):
        """
        Load reference data for PayType mapping
        
        Args:
            ref_file_path: Path to reference Excel file
            
        Returns:
            DataFrame: Reference data as Spark DataFrame
        """
        try:
            # Load Excel file using pandas first, then convert to Spark DataFrame
            ref_pandas = pd.read_excel(ref_file_path)
            ref_df = self.spark.createDataFrame(ref_pandas)
            
            print("Reference data loaded successfully:")
            ref_df.show()
            
            return ref_df
        except Exception as e:
            print(f"Error loading reference data: {e}")
            # Create fallback reference data if file doesn't exist
            fallback_data = [(0, "Prepaid"), (1, "Postpaid")]
            schema = StructType([
                StructField("PayType", IntegerType(), True),
                StructField("value", StringType(), True)
            ])
            return self.spark.createDataFrame(fallback_data, schema)

    def setup_streaming_source(self, data_path="REF_SMS"):
        """
        Setup streaming source from CSV files
        
        Args:
            data_path: Path to data directory containing CSV files
            
        Returns:
            DataFrame: Streaming DataFrame
        """
        # Define schema for SMS data
        sms_schema = StructType([
            StructField("RECORD_DATE", StringType(), True),
            StructField("PayType", IntegerType(), True),
            StructField("Debit_amount_42", LongType(), True),
            # Add other columns as needed
        ])
        
        # Create streaming DataFrame
        streaming_df = self.spark \
            .readStream \
            .format("csv") \
            .option("header", "true") \
            .option("path", data_path) \
            .schema(sms_schema) \
            .load()
        
        # Transform the data
        processed_df = streaming_df \
            .withColumn("timestamp", to_timestamp(col("RECORD_DATE"), "HH:mm:ss yyyy/MM/dd")) \
            .withColumn("revenue_toman", col("Debit_amount_42") / 1000) \
            .filter(col("timestamp").isNotNull())
        
        return processed_df

    def generate_daily_revenue_report(self, streaming_df):
        """
        Report 1: Daily Revenue (in Toman)
        Calculate the total daily revenue
        
        Args:
            streaming_df: Input streaming DataFrame
        """
        daily_revenue = streaming_df \
            .withColumn("date", to_date(col("timestamp"))) \
            .groupBy("date") \
            .agg(sum("revenue_toman").alias("total_revenue")) \
            .orderBy("date")
        
        # Write to MinIO
        query = daily_revenue.writeStream \
            .outputMode("complete") \
            .format("csv") \
            .option("header", "true") \
            .option("path", f"{self.output_bucket}/daily_revenue") \
            .option("checkpointLocation", "/tmp/checkpoint_daily") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print("Daily Revenue Report streaming query started...")
        return query

    def generate_15min_revenue_by_paytype(self, streaming_df, ref_df):
        """
        Report 2: 15-Minute Interval Revenue by Pay Type (in Toman)
        Calculate revenue in 15-minute intervals for each paytype
        
        Args:
            streaming_df: Input streaming DataFrame
            ref_df: Reference DataFrame for PayType mapping
        """
        # Create 15-minute windows
        windowed_df = streaming_df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(
                window(col("timestamp"), "15 minutes").alias("time_window"),
                col("PayType")
            ) \
            .agg(sum("revenue_toman").alias("revenue")) \
            .withColumn("RECORD_DATE", col("time_window.start")) \
            .select("RECORD_DATE", "PayType", "revenue") \
            .orderBy("RECORD_DATE", "PayType")
        
        # Write to MinIO
        query = windowed_df.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("header", "true") \
            .option("path", f"{self.output_bucket}/15min_revenue_by_paytype") \
            .option("checkpointLocation", "/tmp/checkpoint_15min") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print("15-Minute Revenue by PayType streaming query started...")
        return query

    def generate_max_min_revenue_report(self, streaming_df):
        """
        Report 3: Max and Min Revenue (in Toman) in 15-Minute Intervals by Pay Type
        Find the maximum and minimum revenue per 15-minute interval for each paytype
        
        Args:
            streaming_df: Input streaming DataFrame
        """
        max_min_revenue = streaming_df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(
                window(col("timestamp"), "15 minutes").alias("time_window"),
                col("PayType")
            ) \
            .agg(
                max("revenue_toman").alias("max_revenue"),
                min("revenue_toman").alias("min_revenue"),
                sum("revenue_toman").alias("total_revenue")
            ) \
            .withColumn("RECORD_DATE", col("time_window.start")) \
            .select("RECORD_DATE", "PayType", "max_revenue", "min_revenue", "total_revenue") \
            .orderBy("RECORD_DATE", "PayType")
        
        # Write to MinIO
        query = max_min_revenue.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("header", "true") \
            .option("path", f"{self.output_bucket}/max_min_revenue") \
            .option("checkpointLocation", "/tmp/checkpoint_maxmin") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print("Max/Min Revenue Report streaming query started...")
        return query

    def generate_revenue_count_with_mapping(self, streaming_df, ref_df):
        """
        Report 4: Revenue and Record Count in 15-Minute Intervals by Pay Type (in Toman)
        Calculate both the revenue and the number of records in each 15-minute interval
        for each paytype with meaningful status mapping
        
        Args:
            streaming_df: Input streaming DataFrame
            ref_df: Reference DataFrame for PayType mapping
        """
        # First create the windowed aggregation
        windowed_agg = streaming_df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(
                window(col("timestamp"), "15 minutes").alias("time_window"),
                col("PayType")
            ) \
            .agg(
                count("*").alias("Record_Count"),
                sum("revenue_toman").alias("Revenue")
            ) \
            .withColumn("RECORD_DATE", col("time_window.start")) \
            .select("RECORD_DATE", "PayType", "Record_Count", "Revenue")
        
        # Join with reference data to get meaningful PayType names
        # Note: In streaming, we need to broadcast the reference data
        ref_broadcast = broadcast(ref_df)
        
        result_df = windowed_agg \
            .join(ref_broadcast, "PayType", "left") \
            .select(
                col("RECORD_DATE"),
                coalesce(col("value"), col("PayType").cast("string")).alias("Pay_Type"),
                col("Record_Count"),
                col("Revenue")
            ) \
            .orderBy("RECORD_DATE", "Pay_Type")
        
        # Write to MinIO
        query = result_df.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("header", "true") \
            .option("path", f"{self.output_bucket}/revenue_count_with_mapping") \
            .option("checkpointLocation", "/tmp/checkpoint_mapping") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print("Revenue and Count with Mapping streaming query started...")
        return query

    def run_all_analytics(self):
        """
        Main method to run all analytics reports
        """
        print("Starting Spark Structured Streaming Analytics...")
        
        try:
            # Load reference data
            ref_df = self.load_reference_data()
            
            # Setup streaming source
            streaming_df = self.setup_streaming_source()
            
            print("Streaming DataFrame schema:")
            streaming_df.printSchema()
            
            # Start all streaming queries
            queries = []
            
            # Report 1: Daily Revenue
            query1 = self.generate_daily_revenue_report(streaming_df)
            queries.append(query1)
            
            # Report 2: 15-Minute Revenue by PayType
            query2 = self.generate_15min_revenue_by_paytype(streaming_df, ref_df)
            queries.append(query2)
            
            # Report 3: Max/Min Revenue
            query3 = self.generate_max_min_revenue_report(streaming_df)
            queries.append(query3)
            
            # Report 4: Revenue and Count with Mapping
            query4 = self.generate_revenue_count_with_mapping(streaming_df, ref_df)
            queries.append(query4)
            
            print("All streaming queries started successfully!")
            print("Data will be written to MinIO bucket: analytics-output")
            print("\nPress Ctrl+C to stop all queries...")
            
            # Wait for all queries to finish
            for query in queries:
                query.awaitTermination()
                
        except Exception as e:
            print(f"Error running analytics: {e}")
        finally:
            self.spark.stop()

    def stop_all_queries(self):
        """
        Stop all active streaming queries
        """
        for query in self.spark.streams.active:
            query.stop()
        print("All streaming queries stopped.")

# Usage Example
if __name__ == "__main__":
    # Initialize the analytics engine
    analytics = SparkStreamingAnalytics(
        minio_endpoint="localhost:9000",  # Update with your MinIO endpoint
        minio_access_key="minioadmin",    # Update with your access key
        minio_secret_key="minioadmin"     # Update with your secret key
    )
    
    # Run all analytics
    analytics.run_all_analytics()