# 🚀 ALICE Data Lakehouse

A distributed data lakehouse system designed for processing and analyzing infrastructure monitoring data from the ALICE experiment at CERN. The platform provides researchers with advanced tools for data collection, storage, and analysis, enabling the processing of high-volume, heterogeneous data streams from grid computing systems.

---

## 🌟 Key Features
- **Distributed Data Collection**: Real-time processing of large-scale infrastructure data.
- **Scalable Architecture**: Supports both structured and unstructured data storage and analysis.
- **Version Control for Datasets**: Git-like management enabling collaborative research.
- **Web-Based SQL Interface**: Optimized for scientific queries and research workflows.
- **Automated ETL Pipelines**: Asynchronous data processing with scheduled workflows.
- **Multi-Level Access**: Designed for both technical and non-technical users.

---

## 🛠️ Architecture Overview

The system consists of multiple containers, each responsible for specific components of the data lakehouse:

### **Services Overview**

| Service       | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| **Postgres**  | Stores metadata for the Hive Metastore.                                     |
| **Metastore** | Handles table definitions and metadata for data stored in the data lake.    |
| **Trino**     | SQL query engine for querying data across various storage backends.         |
| **Spark**     | Distributed data processing engine for ETL and machine learning workflows.  |
| **Spark Worker** | Executes distributed tasks managed by the Spark master node.            |

---

## 🐳 Dockerized Services

Below is the description of the architecture and the Docker Compose setup:

### **Postgres**
- **Purpose**: Metadata storage for Hive Metastore.
- **Image**: `postgres:13`
- **Environment Variables**:
  - `POSTGRES_DB=metastore`
  - `POSTGRES_USER=postgres`
  - `POSTGRES_PASSWORD=postgres`

---

### **Hive Metastore**
- **Purpose**: Central metadata repository for data lake.
- **Image**: Custom image `my-hive-metastore:latest`
- **Environment Variables**:
  - `DB_DRIVER=postgres`
  - `METASTORE_DB_HOSTNAME=postgres`
  - `METASTORE_DB_PORT=5432`
  - `METASTORE_DB_NAME=metastore`
  - `METASTORE_DB_USER=postgres`
  - `METASTORE_DB_PASSWORD=postgres`

---

### **Trino**
- **Purpose**: SQL query engine for large-scale data analysis.
- **Image**: `trinodb/trino:426`
- **Environment Variables**:
  - `AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY}`
  - `AWS_SECRET_ACCESS_KEY=${AWS_SECRET_KEY}`
  - `AWS_ENDPOINT=https://s3p.cloud.cyfronet.pl`
  - `AWS_DEFAULT_REGION=us-west-2`
  - `TRINO_ENDPOINT=http://trinodb:8080`

---

### **Spark**
- **Purpose**: Distributed data processing engine.
- **Image**: `bitnami/spark:3.4.1`
- **Mode**: Master and Worker nodes
- **Environment Variables**:
  - `SPARK_MODE=master` (for the master container)
  - `SPARK_MODE=worker` (for worker containers)
  - `SPARK_MASTER_HOST=spark`
  - `SPARK_MASTER_PORT=7077`

---

### **Networks**
All services communicate over a dedicated Docker network named `dldg`.

---

## 📦 Environment Variables

The system requires the following environment variables to be configured in a `.env` file:

```plaintext
AWS_ACCESS_KEY=your_access_key
AWS_SECRET_KEY=your_secret_key
AWS_ENDPOINT=https://s3p.cloud.cyfronet.pl
AWS_REGION=us-west-2
METASTORE_DB_USER=postgres
METASTORE_DB_PASSWORD=postgres
TRINO_ENDPOINT=http://trinodb:8080
```
---

## 🚀 Quick Start Guide

Follow these steps to set up and run the ALICE Data Lakehouse platform on your local machine.

---

### ⚙️ Step 1: Install Prerequisites
Before starting, ensure the following tools are installed on your system:
1. **Docker**: [Get Docker](https://docs.docker.com/get-docker/)
2. **Docker Compose**: [Install Docker Compose](https://docs.docker.com/compose/install/)

---

### 📝 Step 2: Clone the Repository
Clone the repository to your local machine:
```bash
git clone https://github.com/your-repo/alice-data-lakehouse.git
cd alice-data-lakehouse
```
---

### 🔧 Step 3: Configure Environment Variables
To properly configure the environment, you need to create a `.env` file in the root directory of the project and define the required environment variables. Here's an example:

```plaintext
AWS_ACCESS_KEY=your_access_key
AWS_SECRET_KEY=your_secret_key
AWS_ENDPOINT=https://s3p.cloud.cyfronet.pl
AWS_REGION=us-west-2
METASTORE_DB_USER=postgres
METASTORE_DB_PASSWORD=postgres
TRINO_ENDPOINT=http://trinodb:8080
```

---

### ▶️ Step 4: Start the Dockerized Environment

To launch the ALICE Data Lakehouse platform, navigate to the root directory of the project and start all services using Docker Compose:

```bash
docker-compose build
docker-compose up -d
```

---
### 🔍 Step 5: Verify the Setup

After starting the containers, verify that all services are running and accessible.


#### ✅ Check Running Containers
Run the following command to list all running containers:
```bash
docker ps
```

#### ✅ Connect to Trino
```bash
docker exec -it trinodb trino
```

