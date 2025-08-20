
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import os
from pyspark.sql.functions import col, sum, to_date

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
        
    def load_reference_data(self, ref_file_path="~/projects/Proj_Moh/Attachments/REF_SMS/Ref.xlsx"):
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

    def setup_streaming_source(self, data_path="~/projects/Proj_Moh/Attachments/REF_SMS/REF_CBS_SMS2.csv"):
       
        # Setup streaming source from CSV files
       
        data_path = os.path.expanduser(data_path)
        # Define schema for SMS data
        sms_schema = StructType([
            StructField("ROAMSTATE_519", IntegerType(), True),
            StructField("CUST_LOCAL_START_DATE_15", LongType(), True),  # was too big for Integer
            StructField("CDR_ID_1", StringType(), True),                # scientific notation
            StructField("CDR_SUB_ID_2", IntegerType(), True),
            StructField("CDR_TYPE_3", StringType(), True),
            StructField("SPLIT_CDR_REASON_4", StringType(), True),
            StructField("RECORD_DATE", StringType(), True),
            StructField("PAYTYPE_515", IntegerType(), True),
            StructField("DEBIT_AMOUNT_42", DecimalType(10, 3), True),
            StructField("SERVICEFLOW_498", IntegerType(), True),
            StructField("EVENTSOURCE_CATE_17", StringType(), True),
            StructField("USAGE_SERVICE_TYPE_19", IntegerType(), True),
            StructField("SPECIALNUMBERINDICATOR_534", DoubleType(), True),
            StructField("BE_ID_30", DoubleType(), True),
            StructField("CALLEDPARTYIMSI_495", StringType(), True),
            StructField("CALLINGPARTYIMSI_494", StringType(), True)     # too long for LongType
        ])
        

        
        df = self.spark.read \
        .option("header", "true") \
        .option("ignoreLeadingWhiteSpace", True) \
        .option("ignoreTrailingWhiteSpace", True) \
        .option("nullValue", "") \
        .option("treatEmptyValuesAsNulls", True) \
        .schema(sms_schema) \
        .csv(data_path)

        # Transform the data
        processed_df = df \
            .withColumn("timestamp", to_timestamp(col("RECORD_DATE"),"yyyy/MM/dd HH:mm:ss" )) \
            .withColumn("revenue_toman", (col("DEBIT_AMOUNT_42")/10000).cast("decimal(6,3)")) \
            .filter(col("timestamp").isNotNull())
        
        return processed_df

    # def generate_daily_revenue_report(self, streaming_df):
    def generate_daily_revenue_report(self,df):
        # Report 1: Daily Revenue (in Toman)
    

        # Method 1: Using Streaming CSV read/write
        # daily_revenue = streaming_df \
        #     .withColumn("date", to_date(col("timestamp"))) \
        #     .groupBy("date") \
        #     .agg(sum("revenue_toman").alias("total_revenue")) \
        #     .orderBy("date")
        
        # # Write to MinIO
        # query = daily_revenue.writeStream \
        #     .outputMode("complete") \
        #     .format("csv") \
        #     .option("header", "true") \
        #     .option("path", f"{self.output_bucket}/daily_revenue") \
        #     .option("checkpointLocation", "/tmp/checkpoint_daily") \
        #     .trigger(processingTime='30 seconds') \
        #     .start()

        # Method 2: Using Static CSV read/write 
        # Compute daily revenue
        daily_revenue = df \
            .withColumn("date", to_date(col("timestamp"))) \
            .groupBy("date") \
            .agg(sum("revenue_toman").alias("total_revenue")) \
            .orderBy("date")

        # Save the result as CSV (not MinIO)
        daily_revenue.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("/home/mohammad/projects/Proj_Moh/outputs/daily_revenue")

        # Save the result as CSV (in MinIO)
        output_path = f"{self.output_bucket}/daily_revenue"
        query = daily_revenue.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"Daily Revenue Report saved to both {output_path} and /home/mohammad/projects/Proj_Moh/outputs/daily_revenue")
        return query

    def generate_15min_revenue_by_paytype(self, streaming_df, ref_df):
        """
        Report 2: 15-Minute Interval Revenue by Pay Type (in Toman)
        Calculate revenue in 15-minute intervals for each paytype
        
        """
        # Create 15-minute windows
        windowed_df = streaming_df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(
                window(col("timestamp"), "15 minutes").alias("time_window"),
                col("PAYTYPE_515")
            ) \
            .agg(sum("revenue_toman").alias("revenue")) \
            .withColumn("RECORD_DATE", col("time_window.start")) \
            .select("RECORD_DATE", "PAYTYPE_515", "revenue") \
            .orderBy("RECORD_DATE", "PAYTYPE_515")
        
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
                col("PAYTYPE_515")
            ) \
            .agg(
                max("revenue_toman").alias("max_revenue"),
                min("revenue_toman").alias("min_revenue"),
                sum("revenue_toman").alias("total_revenue")
            ) \
            .withColumn("RECORD_DATE", col("time_window.start")) \
            .select("RECORD_DATE", "PAYTYPE_515", "max_revenue", "min_revenue", "total_revenue") \
            .orderBy("RECORD_DATE", "PAYTYPE_515")
        
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
                col("PAYTYPE_515")
            ) \
            .agg(
                count("*").alias("Record_Count"),
                sum("revenue_toman").alias("Revenue")
            ) \
            .withColumn("RECORD_DATE", col("time_window.start")) \
            .select("RECORD_DATE", "PAYTYPE_515", "Record_Count", "Revenue")
        
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
            
            print('Query 1 is starting:')
            # Report 1: Daily Revenue
            query1 = self.generate_daily_revenue_report(streaming_df)
            # query1 = self.generate_daily_revenue_report(streaming_df)
            queries.append(query1)
            
            print('Query 2 is starting:')
            # Report 2: 15-Minute Revenue by PayType
            query2 = self.generate_15min_revenue_by_paytype(streaming_df, ref_df)
            queries.append(query2)
            
            print('Query 3 is starting:')
            # Report 3: Max/Min Revenue
            query3 = self.generate_max_min_revenue_report(streaming_df)
            queries.append(query3)
            
            print('Query 4 is starting:')
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
        minio_endpoint="localhost:9000",  
        minio_access_key="minioadmin",    
        minio_secret_key="minioadmin"    
    )
    
    # Run all analytics
    analytics.run_all_analytics()