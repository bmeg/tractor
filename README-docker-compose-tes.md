
## Steps to Install Funnel + MinIO with Docker Compose

### 1. Install Docker & Docker-Compose

Ensure you have **Docker** and **Docker-Compose** installed:

```sh
docker --version
docker-compose --version
```

### Step 3: Download the `docker-compose-tes.yaml` File

Ensure you have the `docker-compose-tes.yaml` file in your project directory.

### Step 4: Start the Funnel Services

Run:

```sh
docker-compose -f docker-compose-tes.yaml up -d
```

This starts the following containers:
- `funnel`: Funnel server.
- `minio`: MinIO server for S3 API.

### Step 5: Access the Funnel Web UI

http://localhost:8000

## 2. Managing Funnel with Docker-Compose

### Launch with Airflow

```sh
docker compose -f docker-compose.yaml -f docker-compose-tes.yaml up
```

### Check Expected Containers

```sh
docker compose -f docker-compose.yaml -f docker-compose-tes.yaml ps

NAME                          IMAGE                                      SERVICE             PORTS
tractor-airflow-scheduler-1   apache/airflow:2.10.4                      airflow-scheduler   8080/tcp
tractor-airflow-triggerer-1   apache/airflow:2.10.4                      airflow-triggerer   8080/tcp
tractor-airflow-webserver-1   apache/airflow:2.10.4                      airflow-webserver   0.0.0.0:8080->8080/tcp
tractor-airflow-worker-1      apache/airflow:2.10.4                      airflow-worker      8080/tcp
tractor-funnel-1              quay.io/ohsu-comp-bio/funnel:development   funnel              0.0.0.0:8000->8000/tcp, 0.0.0.0:9090->9090/tcp
tractor-minio-1               minio/minio:latest                         minio               0.0.0.0:9000-9001->9000-9001/tcp
tractor-postgres-1            postgres:13                                postgres            5432/tcp
tractor-redis-1               redis:7.2-bookworm                         redis               6379/tcp
```

### Check Connection with Funnel

```sh
docker compose exec airflow-webserver /bin/bash

(airflow) curl funnel:8000/service-info
{
  "contactUrl":  "https://ohsu-comp-bio.github.io/funnel/",
  "createdAt":  "2016-03-21T16:27:49-07:00",
  "description":  "Funnel is a toolkit for distributed task execution via a simple, standard API.",
  ...
}
```

### Check Connection with MinIO

```sh
docker compose exec airflow-webserver /bin/bash

curl -I minio:9000/minio/health/live
HTTP/1.1 200 OK
```
