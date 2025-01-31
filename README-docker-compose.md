
## **1. Steps to Install Airflow with Docker-Compose**
### **Step 1: Install Docker & Docker-Compose**
Ensure you have **Docker** and **Docker-Compose** installed:
```sh
docker --version
docker-compose --version
```

### **Step 2: Set Environment Variables**
Apache Airflow requires specific environment variables. Create an `.env` file:
```sh
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```
This ensures that files created by Airflow match your user permissions.

### **Step 3: Download the `docker-compose.yaml` File**
Run:
```sh
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
```
This downloads the official **Airflow `docker-compose.yaml`**.

### **Step 4: Initialize the Airflow Database**
Run:
```sh
docker-compose up airflow-init
```
This initializes the **PostgreSQL metadata database** for Airflow.

### **Step 5: Start the Airflow Services**
Run:
```sh
docker-compose up -d
```
This starts the following containers:
- `airflow-webserver`: UI to manage DAGs.
- `airflow-scheduler`: Triggers and runs tasks.
- `airflow-worker`: Executes tasks.
- `airflow-triggerer`: Handles deferrable operators.
- `airflow-postgres`: Stores metadata.
- `airflow-redis`: Used for task queuing.

### **Step 6: Access the Airflow Web UI**
Visit:
```
http://localhost:8080
```
Login with:
- **Username:** `airflow`
- **Password:** `airflow`

---

## **2. Managing Airflow with Docker-Compose**
### **Stop Airflow**
```sh
docker-compose down
```
### **Restart Airflow**
```sh
docker-compose up -d
```
### **View Logs**
```sh
docker-compose logs -f
```
### **List Running Containers**
```sh
docker ps
```

---

## **3. Why Use `docker-compose` for Airflow?**
✔ **Easy Setup:** Pre-configured `docker-compose.yaml`.  
✔ **Isolated Environment:** No need to install dependencies locally.  
✔ **Scalability:** Can be extended for production setups.  
✔ **Persistence:** Keeps DAGs and logs even after stopping.

---

### **🔥 TL;DR**
**Yes, installing Airflow with `docker-compose` is easy!** Just:
1. **Download `docker-compose.yaml`**
2. **Run `docker-compose up airflow-init`**
3. **Start services with `docker-compose up -d`**
4. **Access Airflow UI at `http://localhost:8080`**

🚀 **You're now running Airflow in Docker!**
