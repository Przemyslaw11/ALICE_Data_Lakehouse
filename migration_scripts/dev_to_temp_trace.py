from config import AWS_CONFIG
from logger import setup_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, to_timestamp, year

logger = setup_logger(__name__)

def create_spark_session():
    """Create and configure Spark session."""
    logger.info("Configuring Spark session...")
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
    logger.info("Starting script execution...")
    
    try:
        spark = create_spark_session()
        
        # Define your buckets
        input_bucket = "s3a://alice-data-lake-dev"
        output_bucket = "s3a://alice-data-lake-temp/trace"
        trace_folders = [
            "trace_01ab0f63-ab03-4053-bce7-b644b2adb180",
            "trace_54de94b3-3079-4ae3-9f46-86289dda24d2",
            "trace_656fa379-226f-486d-a7c8-c2d9fae5e2c7",
        ]

        dfs = []
        for folder in trace_folders:
            logger.info(f"Processing folder: {folder}")
            input_path = f"{input_bucket}/{folder}/data/*.parquet"
            
            try:
                df = spark.read.format("parquet").load(input_path)
                file_count = df.count()
                logger.info(f"Found {file_count} records in {folder}")
                
                if file_count > 0:
                    # Add partition columns
                    df = df.withColumn(
                        "year",
                        year(to_timestamp(col("timestamp"))),
                    )
                    df = df.withColumn(
                        "month",
                        month(to_timestamp(col("timestamp"))),
                    )
                    
                    # Add to list of dataframes
                    dfs.append(df)
                    logger.info(f"Successfully processed {folder}")
                else:
                    logger.warning(f"No data found in {folder}")
                    
            except Exception:
                logger.exception(f"Error processing folder {folder}")
                continue

        if dfs:
            # Union all dataframes
            final_df = dfs[0]
            for df in dfs[1:]:
                final_df = final_df.union(df)

            # Write to Delta format
            logger.info(f"Writing data to Delta format at {output_bucket}")
            (final_df.write
                .format("delta")
                .partitionBy("year", "month")
                .mode("overwrite")
                .save(output_bucket))
            
            logger.info("Data successfully written to Delta format")
        else:
            logger.warning("No data to process")

    except Exception:
        logger.exception("Error occurred during processing")
        raise
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Script completed")

if __name__ == "__main__":
    main()