from config import AWS_CONFIG
from pyspark.sql import SparkSession


def create_spark_session():
    """Create and configure Spark session."""
    print("Configuring Spark session...")
    return (SparkSession.builder
        .appName("ReadDeltaFromS3")
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
    """Main function to read Delta table."""
    print("Starting Delta table read operation...")
   
    try:
        spark = create_spark_session()
       
        # Path to the Delta table
        delta_table_path = "s3a://alice-data-lake-temp/raw/trace"
       
        print(f"Reading Delta table from: {delta_table_path}")
       
        # Read the Delta table
        df = spark.read.format("delta").load(delta_table_path)
       
        # Show table info
        print("Delta table schema:")
        df.printSchema()
       
        print("Row count:")
        count = df.count()
        print(f"Total number of rows: {count}")
       
        # Show sample data
        print("Sample data:")
        df.show(5)
    except Exception as e:
        print(f"Error reading Delta table: {str(e)}")
        raise
    finally:
        print("Stopping Spark session...")
        spark.stop()
        print("Operation completed")


if __name__ == "__main__":
    main()
