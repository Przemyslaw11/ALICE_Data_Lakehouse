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
        .appName("MonJDLSParsedToDelta")
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
    """Main function to convert parquet files to Delta format."""
    print("Starting script execution...")
    
    try:
        spark = create_spark_session()
        
        input_bucket = "s3a://alice-data-lake-dev"
        output_path = "s3a://alice-data-lake-temp/processed/mon_jdls_parsed"
        
        mon_jdls_folders = [
            "mon_jdls_parsed",
            "mon_jdls_parsed2_6376478e-c48a-446a-a2fb-0f3dba64f42c",
            "mon_jdls_parsed_69ae7404-12e3-4a67-9f3d-b60ab41137c9",
            "mon_jdls_parsed_bb9eeb2a-eb28-4032-aae9-cb0c550c1ee6"
        ]
        
        dfs = []
        for folder in mon_jdls_folders:
            print(f"Processing folder: {folder}")
            input_path = f"{input_bucket}/{folder}/data/*.parquet"
            
            try:
                df = spark.read.format("parquet").load(input_path)
                file_count = df.count()
                print(f"Found {file_count} records in {folder}")
                
                if file_count > 0:
                    dfs.append(df)
                    print(f"Successfully processed {folder}")
                else:
                    print(f"No data found in {folder}")
                
            except Exception as e:
                print(f"Error processing folder {folder}: {str(e)}")
                continue

        if dfs:
            final_df = dfs[0]
            for df in dfs[1:]:
                final_df = final_df.union(df)

            print(f"Writing data to Delta format at {output_path}")
            final_df.write.format("delta").mode("overwrite").save(output_path)
            
            print("Data successfully written to Delta format")
        else:
            print("No data to process")

    except Exception as e:
        print(f"Error occurred during processing: {str(e)}")
        raise
    finally:
        print("Stopping Spark session...")
        spark.stop()
        print("Script completed")

if __name__ == "__main__":
    main()