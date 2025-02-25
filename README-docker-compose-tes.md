
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

### Stop Funnel

```sh
docker-compose -f docker-compose-tes.yaml down
```

### Restart Funnel

```sh
docker-compose -f docker-compose-tes.yaml up -d
```

### View Logs

```sh
docker-compose -f docker-compose-tes.yaml logs -f
```

### List Running Containers
```sh
docker ps
```
