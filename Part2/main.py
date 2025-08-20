
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import os

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
        # Load reference data for PayType mapping
        
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
        
        daily_revenue.show(10)

        # Save the result as CSV (in MinIO)
        output_path = f"{self.output_bucket}/daily_revenue"
        query = daily_revenue.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"Daily Revenue Report saved to both {output_path} and /home/mohammad/projects/Proj_Moh/outputs/daily_revenue")
        return query

    def generate_15min_revenue_by_paytype(self, df):

        # Truncate timestamp to 15-minute intervals
        revenue_15min = df \
            .withWatermark("timestamp", "1 day") \
            .groupBy(
                window(col("timestamp"), "15 minutes"), 
                col("PAYTYPE_515")
            ) \
            .agg(sum("revenue_toman").alias("revenue")) \
            .withColumn("interval_start", col("window.start")) \
            .drop("window") \
            .orderBy("interval_start", "PAYTYPE_515")

        revenue_15min.show(10)
        # Save to local filesystem
        revenue_15min.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("/home/mohammad/projects/Proj_Moh/outputs/revenue_15min_by_paytype")

        # Save to MinIO
        output_path = f"{self.output_bucket}/revenue_15min_by_paytype"
        query = revenue_15min.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        print(f"15-Minute Revenue by PayType saved to both {output_path} and /home/mohammad/projects/Proj_Moh/outputs/revenue_15min_by_paytype")
        return query



    def generate_max_min_revenue_report(self, streaming_df):

        # Group into 15-minute intervals and calculate min/max revenue per paytype
        revenue_15min_minmax = streaming_df \
            .withWatermark("timestamp", "1 day") \
            .groupBy(
                window(col("timestamp"), "15 minutes"),
                col("PAYTYPE_515")
            ) \
            .agg(
                min("revenue_toman").alias("min_revenue"),
                max("revenue_toman").alias("max_revenue")
            ) \
            .withColumn("interval_start", col("window.start")) \
            .drop("window") \
            .orderBy("interval_start", "PAYTYPE_515")

        # Save to local filesystem
        revenue_15min_minmax.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("/home/mohammad/projects/Proj_Moh/outputs/revenue_15min_minmax_by_paytype")

        revenue_15min_minmax.show(20)
        # Save to MinIO
        output_path = f"{self.output_bucket}/revenue_15min_minmax_by_paytype"
        query = revenue_15min_minmax.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        print(f"15-Minute Min/Max Revenue by PayType saved to both {output_path} and /home/mohammad/projects/Proj_Moh/outputs/revenue_15min_minmax_by_paytype")
        return query

    def generate_revenue_count_with_mapping(self, streaming_df, ref_df):


      # Join reference mapping (Prepaid/Postpaid)
        df_with_ref = streaming_df.join(
            ref_df,
            streaming_df["PAYTYPE_515"] == ref_df["PayType"],
            how="left"
        )

        # Group into 15-minute intervals, compute record count and revenue
        revenue_count_15min = df_with_ref \
            .withWatermark("timestamp", "1 day") \
            .groupBy(
                window(col("timestamp"), "15 minutes"),
                col("value")  # mapped PayType name (Prepaid/Postpaid)
            ) \
            .agg(
                count("*").alias("record_count"),
                sum("revenue_toman").alias("revenue")
            ) \
            .withColumn("interval_start", col("window.start")) \
            .drop("window") \
            .orderBy("interval_start", "value")
        
        revenue_count_15min.show(10)

        # Save to local filesystem
        revenue_count_15min.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("/home/mohammad/projects/Proj_Moh/outputs/revenue_count_15min_by_paytype")

        # Save to MinIO
        output_path = f"{self.output_bucket}/revenue_count_15min_by_paytype"
        query = revenue_count_15min.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)

        print(f"15-Minute Revenue + Count by PayType saved to both {output_path} and /home/mohammad/projects/Proj_Moh/outputs/revenue_count_15min_by_paytype")
        return query

    def run_all_analytics(self):
        
        # Main method to run all analytics reports
        
        print("Starting Spark Structured Streaming Analytics...")
        
        try:
        
            # Load reference data => For the PayType values
            ref_df = self.load_reference_data()
            
            streaming_df = self.setup_streaming_source()
            
            print("Streaming DataFrame schema:")
            streaming_df.printSchema()
            
            print('Query 1 is starting:')
            # Report 1: Daily Revenue
            self.generate_daily_revenue_report(streaming_df)
            
            print('Query 2 is starting:')
            # Report 2: 15-Minute Revenue by PayType
            self.generate_15min_revenue_by_paytype(streaming_df)
            
            print('Query 3 is starting:')
            # Report 3: Max/Min Revenue
            self.generate_max_min_revenue_report(streaming_df)
            
            print('Query 4 is starting:')
            # Report 4: Revenue and Count with Mapping
            self.generate_revenue_count_with_mapping(streaming_df, ref_df)
            
            print("All streaming queries started successfully!")
            print("Data is written to MinIO bucket: analytics-output")
            print("\nPress Ctrl+C to stop all queries...")
            
                
        except Exception as e:
            print(f"Error running analytics: {e}")
        finally:
            self.spark.stop()


if __name__ == "__main__":

    # Initialize the analytics engine
    analytics = SparkStreamingAnalytics(
        minio_endpoint="localhost:9000",  
        minio_access_key="minioadmin",    
        minio_secret_key="minioadmin"    
    )
    
    analytics.run_all_analytics()