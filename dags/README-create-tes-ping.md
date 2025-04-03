# create_tes_ping

## Overview
create_tes_ping.py is a Python script designed to create Directed Acyclic Graphs (DAGs) for running in an Apache Airflow environment. 

![Image](https://github.com/user-attachments/assets/240d91cf-1356-4e99-a484-9c123c609ff5)

## Features
* pings the TES server to verify that it is running, using a BashOperator curl command
* sends a test job to the TES server using a TESOperator 
* TODO - verifies that the job's output was uploaded to the specified bucket


## Usage
* Place the `tractor` directory in the root of your Airflow installation.
* Place the `create_tes_ping.py` script in the root of `dags` directory of your Airflow installation. Run the script to generate the DAGs based on the specified templates and configurations.
* Prompt airflow to refresh the DAGs by running:
```
dc exec -it  airflow-worker airflow  dags reserialize -S /opt/airflow/dags/create_tes_ping.py
```
