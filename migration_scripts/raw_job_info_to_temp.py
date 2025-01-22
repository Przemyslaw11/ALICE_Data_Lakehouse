from pyspark.sql.functions import col, from_unixtime, year, month
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()
print("Loading environment variables...")
print(f"AWS Endpoint: {os.getenv('AWS_ENDPOINT')}")
print(f"AWS Region: {os.getenv('AWS_REGION')}")
print("Access and Secret keys are set:", 
      bool(os.getenv('AWS_ACCESS_KEY')), 
      bool(os.getenv('AWS_SECRET_KEY')))

AWS_CONFIG = {
    'access_key': os.getenv('AWS_ACCESS_KEY'),
    'secret_key': os.getenv('AWS_SECRET_KEY'),
    'endpoint': os.getenv('AWS_ENDPOINT'),
    'region': os.getenv('AWS_REGION')
}

def create_spark_session():
    """Create and configure Spark session."""
    print("Configuring Spark session...")
    try:
        spark = (SparkSession.builder
            .appName("JobInfoToDelta")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.access.key", AWS_CONFIG['access_key'])
            .config("spark.hadoop.fs.s3a.secret.key", AWS_CONFIG['secret_key'])
            .config("spark.hadoop.fs.s3a.endpoint", AWS_CONFIG['endpoint'])
            .config("spark.hadoop.fs.s3a.region", AWS_CONFIG['region'])
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .getOrCreate())
        print("Spark session created successfully")
        return spark
    except Exception as e:
        print(f"Error creating Spark session: {str(e)}")
        raise

def main():
    """Main function to process job_info data migration."""
    print("Starting script execution...")
   
    try:
        spark = create_spark_session()
        print("SparkSession created successfully")
       
        input_bucket = "s3a://alice-data-lake-dev"
        output_bucket = "s3a://alice-data-lake-temp/raw/job_info"
        
        job_info_folders = [
            "job_info_13dc9522-e02a-4acd-96eb-5e52b93adb86",
            "job_info_7b4f79e9-203a-42d8-b11c-a74c9c0dd431",
            "job_info_9744140f-d8d0-47f1-9128-90829dcc2e58",
            "job_info_db4fedc1-9c80-45eb-ab8a-8835b48fdcf4"
        ]

        dfs = []
        for folder in job_info_folders:
            print(f"Processing folder: {folder}")
            input_path = f"{input_bucket}/{folder}/data/*.parquet"
            print(f"Reading from path: {input_path}")
           
            try:
                df = spark.read.format("parquet").load(input_path)
                file_count = df.count()
                print(f"Found {file_count} records in {folder}")
               
                if file_count > 0:
                    df = df.withColumn(
                        "year",
                        year(from_unixtime(col("job_submit_timestamp")))
                    )
                    df = df.withColumn(
                        "month",
                        month(from_unixtime(col("job_submit_timestamp")))
                    )
                    
                    dfs.append(df)
                    print(f"Successfully processed {folder}")
                else:
                    print(f"No data found in {folder}")
                   
            except Exception as e:
                print(f"Error processing folder {folder}: {str(e)}")
                continue

        if dfs:
            print("Combining dataframes...")
            final_df = dfs[0]
            for df in dfs[1:]:
                final_df = final_df.union(df)

            print(f"Writing data to Delta format at {output_bucket}")
            (final_df.write
                .format("delta")
                .partitionBy("year", "month")
                .mode("overwrite")
                .save(output_bucket))
           
            print("Data successfully written to Delta format")
        else:
            print("No data to process")

    except Exception as e:
        print(f"Error occurred during processing: {str(e)}")
        import traceback
        print(traceback.format_exc())
        raise
    finally:
        if 'spark' in locals():
            print("Stopping Spark session...")
            spark.stop()
        print("Script completed")

if __name__ == "__main__":
    main()