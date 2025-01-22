from pyspark.sql.functions import col, month, year, from_unixtime
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

AWS_CONFIG = {
    'access_key': os.getenv('AWS_ACCESS_KEY'),
    'secret_key': os.getenv('AWS_SECRET_KEY'),
    'endpoint': os.getenv('AWS_ENDPOINT'),
    'region': os.getenv('AWS_REGION')
}


def create_spark_session():
    """Create and configure Spark session."""
    print("Configuring Spark session...")
    return (SparkSession.builder
        .appName("S3ToS3Delta")
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

def main():
    """Main function to process data migration."""
    print("Starting script execution...")
    
    try:
        spark = create_spark_session()
        
        input_bucket = "s3a://alice-data-lake-dev"
        output_bucket = "s3a://alice-data-lake-temp/raw/trace"
        
        trace_folders = [
            "trace_01ab0f63-ab03-4053-bce7-b644b2adb180",
            "trace_54de94b3-3079-4ae3-9f46-86289dda24d2",
            "trace_656fa379-226f-486d-a7c8-c2d9fae5e2c7",
        ]
        
        dfs = []
        for folder in trace_folders:
            print(f"Processing folder: {folder}")
            input_path = f"{input_bucket}/{folder}/data/*.parquet"
            
            try:
                df = spark.read.format("parquet").load(input_path)
                file_count = df.count()
                print(f"Found {file_count} records in {folder}")
                
                if file_count > 0:
                    df = df.withColumn(
                        "timestamp_converted",
                        from_unixtime(col("finaltimestamp")/1000)
                    )
                    df = df.withColumn(
                        "year",
                        year(col("timestamp_converted"))
                    )
                    df = df.withColumn(
                        "month",
                        month(col("timestamp_converted"))
                    )
                    
                    df = df.drop("timestamp_converted")
                    
                    dfs.append(df)
                    print(f"Successfully processed {folder}")
                else:
                    print(f"Warning: No data found in {folder}")
                
            except Exception as e:
                print(f"Error processing folder {folder}: {str(e)}")
                continue

        if dfs:
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
            print("Warning: No data to process")

    except Exception as e:
        print(f"Error occurred during processing: {str(e)}")
        raise
    finally:
        print("Stopping Spark session...")
        spark.stop()
        print("Script completed")

if __name__ == "__main__":
    main()