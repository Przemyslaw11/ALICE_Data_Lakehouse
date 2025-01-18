# ALICE_Data_Lakehouse

A distributed data lakehouse system designed for processing and analyzing infrastructure monitoring data from the ALICE experiment at CERN. The platform provides researchers with powerful tools for data collection, storage, and analysis while handling high-volume, heterogeneous data streams from grid computing systems.

## Key Features
- Distributed data collection system with real-time processing capabilities
- Scalable data lakehouse architecture supporting both structured and unstructured data
- Git-like version control for datasets enabling collaborative research
- Web-based SQL interface optimized for scientific queries
- Asynchronous data processing pipeline with automated ETL workflows
- Multi-level access supporting both technical and non-technical users

## Technical Highlights
- Two-tier architecture separating data ingestion from processing for optimal performance
- Horizontally scalable components with auto-scaling capabilities
- Temporary relational storage for raw data with async migration to data lake
- Advanced data transformation and aggregation pipeline
- Research-oriented query interface with optimization for scientific workloads


# Environment Setup Guide

This guide provides instructions for setting up a development environment with Apache Spark, MinIO (S3-compatible storage), and Trino for data processing and analysis.

## Prerequisites

- Ubuntu-based system
- Internet connectivity
- Sudo privileges

## Basic System Setup

1. Configure DNS (if needed):
   ```bash
   # Edit resolv.conf
   sudo nano /etc/resolv.conf
   
   # Test connectivity
   ping google.com
   ```

2. Update system packages:
   ```bash
   sudo apt update
   ```

## Java Installation

Install OpenJDK 11:
```bash
sudo apt install openjdk-11-jdk
```

## Apache Spark Setup

1. Download required dependencies:
   ```bash
   wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
   wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.533/aws-java-sdk-bundle-1.12.533.jar
   wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
   wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar
   wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar
   ```

2. Install Spark:
   ```bash
   tar -xvf spark-3.4.1-bin-hadoop3.tgz
   sudo mv spark-3.4.1-bin-hadoop3 /usr/local/spark
   ```

3. Configure environment variables:
   ```bash
   sudo -i
   echo 'export SPARK_HOME=/usr/local/spark' >> ~/.bashrc
   echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
   source ~/.bashrc
   ```

4. Verify installation:
   ```bash
   spark-submit --version
   ```

## MinIO Setup

1. Install MinIO server:
   ```bash
   wget https://dl.min.io/server/minio/release/linux-amd64/minio
   chmod +x minio
   ```

2. Start MinIO server:
   ```bash
   ./minio server /local_test/minio-data --console-address ":9001"
   ```

   Default endpoints:
   - API: http://192.168.1.1:9000
   - Console: http://192.168.1.1:9001

   Default credentials:
   - AccessKey: minioadmin
   - SecretKey: minioadmin

3. Configure MinIO client:
   ```bash
   wget https://dl.min.io/client/mc/release/linux-amd64/mc
   chmod +x mc
   ./mc alias set myminio http://localhost:9000 minioadmin minioadmin
   ./mc mb myminio/spark-delta-lake
   ```

## Trino Setup

1. Install required dependencies:
   ```bash
   sudo apt install python3 python3-pip
   
   # Install Docker using convenience script
   curl -fsSL https://get.docker.com -o get-docker.sh
   sudo sh get-docker.sh
   
   # Install Docker Compose
   sudo apt install docker-compose
   ```

2. Verify installations:
   ```bash
   python3 --version
   docker --version
   docker-compose --version
   ```

## Environment Configuration

Create a `.env` file with the following contents:
```
AWS_ACCESS_KEY=your_access_key
AWS_SECRET_KEY=your_secret_key
AWS_ENDPOINT=https://s3p.cloud.cyfronet.pl/
AWS_REGION=us-west-2
METASTORE_HIVE_DIR=s3a://alice-data-lake-temp/
METASTORE_DB_USER=dataeng
METASTORE_DB_PASSWORD=dataengineering_user
TRINO_ENDPOINT=http://trinodb:8080/
MYSQL_USER=dataeng
MYSQL_PASSWORD=dataengineering_user
MYSQL_ROOT_PASSWORD=dataengineering
```

## Running Spark Scripts

Use the following template to run Spark scripts with required dependencies:
```bash
spark-submit \
  --jars /local_test/data/spark/delta-core_2.12-2.4.0.jar,\
/local_test/data/spark/delta-storage-2.4.0.jar,\
/local_test/data/spark/hadoop-aws-3.3.4.jar,\
/local_test/data/spark/aws-java-sdk-bundle-1.12.533.jar \
  /path/to/your/script.py
```

## Working with Delta Tables

### Download specific files for inspection:
```bash
aws s3 cp s3://alice-data-lake-temp/trace/year=2024/month=3/part-00221-dc29e0bf-55d2-477c-8b13-a0db388802ef.c000.snappy.parquet \
    /path/to/local/directory \
    --profile alice
```

### Create Delta table on existing bucket:
```sql
CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'trace',
    table_location => 's3a://alice-data-lake-temp/trace/'
);
```

### Create new partitioned Delta table:
```sql
CREATE TABLE delta.default.trace (
    job_id BIGINT,
    aliencpuefficiency VARCHAR,
    cputime VARCHAR,
    host VARCHAR,
    maxrss BIGINT,
    cpuefficiency VARCHAR,
    finaltimestamp TIMESTAMP,
    masterjobid BIGINT,
    pid BIGINT,
    requestedcpus INTEGER,
    requestedttl BIGINT,
    runningtimestamp BIGINT,
    savingtimestamp BIGINT,
    startedtimestamp BIGINT,
    walltime INTEGER,
    maxvirt BIGINT,
    site VARCHAR,
    laststatuschangetimestamp BIGINT,
    year VARCHAR,
    month VARCHAR
) WITH (
    location = 's3a://alice-data-lake-temp/trace/',
    partitioned_by = ARRAY['year', 'month']
);
```