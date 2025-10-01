# Airflow based Data Pipeline with MINIO, and PostgreSQL

## Architecture Diagram

![Architecture Diagram](https://github.com/jacobjoe/airflow-etl-minio-to-postgres/blob/main/assets/images/architecture.jpg)

## Project Overview

This project demonstrates the implementation of an end-to-end ETL pipeline following the Medallion architecture, orchestrated using Apache Airflow. The pipeline ingests raw real estate data from a MinIO object store, performs data cleaning and transformations, and loads the processed and normalized data into a PostgreSQL database.

### Key Technologies:
- **MINIO** - For storing raw and Bronze data.
- **PostgreSQL** - For storing Silver and Gold data.
- **Python** - Used for etl(data transformation and data cleaning).
- **Apache Airflow** - For orchestration and schedule.

## Workflow Overview

1. **Raw Data Storage**:
   - Raw real estate data is stored in **minio** in a separate bucket(**raw-data 🪣**).

2. **Bronze Data Storage**:
   - Files are converted to same file type (csv) and stored in a separet bucket(**bronze 🪣**) in **minio**

3. **Silver Data Storage**:
   - Data is cleaned and stored in PostreSQL in schema **Silver 🛢️**.

4. **Gold Data Storage**:
   - Data is normalized and stored in Postgres in schema **Gold 🛢️**.

5. **Apache Airflow**:
   - Apache airflow used for schedule and orchestration.

## Step by step procedure to set up and run

### 1. Clone the repo
- clone this repository to your local using
```
git clone https://github.com/jacobjoe/airflow-etl-minio-to-postgres.git
```

### 2. Build docker image and run airflow
- Run [docker desktop](https://docs.docker.com/desktop/setup/install/windows-install/)
- Open terminal in the downloaded project folder
- Use the following command to build docker image and run in a background mode
```
docker-compose up -d
```

### 3. Open airflow 
- Open airflow in browser using following location
- http://localhost:8080/

### 4. Run Minio 
- Open terminal in project folder and the following command
```
docker run -p 9000:9000 -p 9001:9001 --name minio -v ${HOME}\minio\data:/data -e "MINIO_ROOT_USER=ROOTNAME" -e "MINIO_ROOT_PASSWORD=CHANGEME123" quay.io/minio/minio server /data --console-address ":9001"
```

### 5. Open Minio 
- Open minio in browser using following location
- http://localhost:9001

### 6. Create a connection between Airflow and Minio 
- Go to 'Admin'
- Go to 'Connections'
- Add a New connection
- Mention a connection id
- Select AWS (as minio is a AWS compatible)
- In Extra Field tab mention the following
```
{
  "aws_access_key_id": "ROOTNAME",
  "aws_secret_access_key": "CHANGEME123",
  "endpoint_url": "http://host.docker.internal:9000",
  "region_name": "us-east-1",
  "use_ssl": false
}
```

### 6. Add Data to Minio
- Create bucket in Mino (bucket name - 'raw-data')
- Create folder inside bucket (folder name - 'raw-files')
- Add both these raw files ([files](https://github.com/jacobjoe/airflow-etl-minio-to-postgres/tree/main/assets/data)) to that folder

### 7. Open DBeaver 
- Open [DBeaver](https://dbeaver.io/download/) and create a connection for Postgres (make connection as localhost)
- Create a new database 'etl-cleaned'

### 8. Create a connection between Airflow and PostgreSQL 
- Go to 'Admin'
- Go to 'Connections'
- Add a New connection
- Mention a connection id
- Select Postgres
- In Fields tab mention the following
```
connection_id: postgres_localhost
connection_type: postgres
schema: etl-cleaned
login: airflow
password: airflow
port: 5432
host:host.docker.internal
```

### 9. Run DAG in Airflow
- Go to the airflow
- Enable the DAG created for etl
