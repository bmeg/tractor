 
# **Best Practices for Python Coding in Airflow DAGs**

When writing **Airflow DAGs** in Python, there are several **best practices** that improve maintainability, reliability, and scalability. Below are key principles, **useful libraries**, and **frameworks** that can help.

---

## **1️⃣ DAG Structure & Organization**
### ✅ **Use Modular Code & Separate Logic**
- Keep the **DAG definition** separate from the **business logic**.
- Use **Python functions** instead of writing logic inside operators.

📌 **Example:**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def fetch_data():
    # Business logic here
    print("Fetching data...")

with DAG(
    dag_id="example_dag",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data
    )
```
---
### ✅ **Use Jinja Templating for Dynamic Tasks**
- Instead of hardcoding values, use **Jinja templating**.
- Example: Dynamic filenames based on execution date.

📌 **Example:**
```python
from airflow.operators.bash import BashOperator

task = BashOperator(
    task_id="templated_task",
    bash_command="echo 'Processing date {{ ds }}'",
    dag=dag,
)
```

---

## **2️⃣ Libraries & Frameworks to Improve Airflow DAGs**
### 🔹 **Use `airflow-utils` for Prebuilt Helpers**
[`astronomer/airflow-utils`](https://github.com/astronomer/airflow-utils) provides:
- Common decorators
- Error handling
- Logging utilities

---

### 🔹 **Use `pydantic` for Parameter Validation**
Enforce **strong typing** and **validate inputs** to DAGs using `pydantic`.

📌 **Example:**
```python
from pydantic import BaseModel

class ETLConfig(BaseModel):
    source: str
    destination: str
    batch_size: int

config = ETLConfig(source="database", destination="s3", batch_size=5000)
```

---

### 🔹 **Use `taskflow` API for Python Tasks**
The **taskflow API** makes DAGs more readable and **manages dependencies automatically**.

📌 **Example:**
```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule_interval="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def example_taskflow():

    @task
    def extract():
        return {"data": "example"}

    @task
    def transform(data):
        return f"Transformed {data}"

    @task
    def load(result):
        print(f"Loading: {result}")

    load(transform(extract()))

dag_instance = example_taskflow()
```

---

## **3️⃣ Scalability & Reliability Best Practices**
### ✅ **Use Sensors for Dependency Handling**
- **ExternalTaskSensor** for waiting on upstream DAGs.
- **S3KeySensor** for waiting on files in S3.

📌 **Example:**
```python
from airflow.sensors.filesystem import FileSensor

file_sensor = FileSensor(
    task_id="wait_for_file",
    filepath="/tmp/myfile.csv",
    poke_interval=30,
    timeout=600,
    dag=dag,
)
```

---

### ✅ **Enable Retries & Alerts**
- Use **retries** to handle transient failures.
- Set **SLAs** and **email alerts** for failures.

📌 **Example:**
```python
from airflow.operators.email import EmailOperator

task = EmailOperator(
    task_id="send_alert",
    to="admin@example.com",
    subject="DAG Failure Alert",
    html_content="DAG failed!",
    dag=dag
)
```

---

## **4️⃣ Dependency Management & Packaging**
### ✅ **Use Virtual Environments for DAG Dependencies**
Avoid installing packages globally. Use a **virtual environment** and freeze dependencies:
```sh
python -m venv airflow_env
source airflow_env/bin/activate
pip install -r requirements.txt
```

---

### ✅ **Use Custom Python Modules in DAGs**
Instead of writing all logic inside the DAG file, store functions in `scripts/` and **import them**.

📌 **Example Project Structure:**
```
/dags/
  my_dag.py
/scripts/
  helpers.py
/requirements.txt
```
📌 **Importing the helper function in DAG:**
```python
from scripts.helpers import fetch_data
```

---

## **5️⃣ Performance Optimizations**
### ✅ **Use `ShortCircuitOperator` to Skip Unnecessary Tasks**
Avoid running tasks if a **condition is not met**.

📌 **Example:**
```python
from airflow.operators.python import ShortCircuitOperator

def check_if_data_available():
    return False  # If False, downstream tasks are skipped

task = ShortCircuitOperator(
    task_id="check_data",
    python_callable=check_if_data_available,
    dag=dag
)
```

---

### ✅ **Use `XComArg` Instead of `xcom_push()` & `xcom_pull()`**
XComArg simplifies passing data between tasks.

📌 **Example:**
```python
from airflow.decorators import task

@task
def extract():
    return "data"

@task
def transform(data):
    return f"Processed {data}"

transform(extract())
```

---

## **✅ Summary: Best Practices for Writing Airflow DAGs**
| **Category**           | **Best Practice** |
|------------------------|------------------|
| **Code Organization** | Keep DAG definitions separate from business logic |
| **Libraries** | Use `pydantic`, `airflow-utils`, `taskflow` API |
| **Error Handling** | Use retries, alerts, SLAs |
| **Dependency Management** | Use virtual environments & modular Python scripts |
| **Performance** | Use `ShortCircuitOperator` and `XComArg` |

🚀 **Following these best practices will make Airflow DAGs more efficient, scalable, and maintainable!**

# **Runtime Abstraction**

[**Astronomer Cosmos**](https://github.com/astronomer/astronomer-cosmos) library abstracts KubernetesPodOperator, DockerOperator, and other container-based operators by providing a common **ContainerTask** interface.

Alternatively, if you need a more generic approach to abstracting operator selection, **Custom Task Flow APIs** and **Factory Patterns** can be used.

---

## **🔹 1. Astronomer Cosmos**
Astronomer Cosmos simplifies container execution across different backends (Kubernetes, Docker, ECS) via **ContainerTask**.

📌 **Install it:**
```sh
pip install astronomer-cosmos
```

📌 **Example: Abstracting KubernetesPodOperator & DockerOperator**
```python
from cosmos import ContainerTask
from airflow.decorators import dag
from datetime import datetime

@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def my_container_dag():

    task = ContainerTask(
        task_id="run_container",
        image="python:3.8",
        cmds=["python", "-c", "print('Hello World')"],
        container_engine="kubernetes",  # Can be "docker", "kubernetes", "ecs"
    )

my_dag = my_container_dag()
```
🔹 **Key Benefits:**
- **Abstracts** KubernetesPodOperator & DockerOperator into a single interface.
- Supports **multiple backends** (`docker`, `kubernetes`, `ecs`).
- Reduces **code duplication**.

---

## **🔹 2. Custom Factory Function (Alternative)**
If you want to manually abstract **operator selection** based on configuration:

📌 **Example: Factory Function to Select the Operator**
```python
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

def create_container_task(task_id, image, use_kubernetes=False):
    if use_kubernetes:
        return KubernetesPodOperator(
            task_id=task_id,
            namespace="default",
            image=image,
            name=task_id,
            in_cluster=True,
            is_delete_operator_pod=True
        )
    else:
        return DockerOperator(
            task_id=task_id,
            image=image,
            auto_remove=True,
            docker_url="unix://var/run/docker.sock"
        )

task = create_container_task("run_container", "python:3.8", use_kubernetes=True)
```
🔹 **Key Benefits:**
- Allows **dynamic operator selection** based on environment.
- More **manual control** than Cosmos.

---

## **🔹 3. Using the TaskFlow API (Alternative)**
If using **Python functions** instead of operators, Airflow's **TaskFlow API** allows you to wrap container execution inside a Python task.

📌 **Example: Using Subprocess for Local Containers**
```python
from airflow.decorators import task

@task
def run_container():
    import subprocess
    subprocess.run(["docker", "run", "--rm", "python:3.8", "python", "-c", "print('Hello World')"])

run_container()
```
🔹 **Key Benefits:**
- **No need for DockerOperator or KubernetesPodOperator**.
- Works well for local container execution.

---

## **✅ Summary: Choosing the Right Abstraction**
| **Approach**        | **Best For** |
|---------------------|-------------|
| **Astronomer Cosmos** ✅ | Best for standardized **container execution** across Kubernetes, Docker, ECS |
| **Factory Function** 🔧 | Best for **manual control** over operator selection |
| **TaskFlow API + Subprocess** 🛠 | Best for simple **local container execution** without Airflow Operators |

🚀 **Recommendation:** If you need **full abstraction**, use **Astronomer Cosmos**. If you need **manual control**, use the **factory function**.