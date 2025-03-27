FROM apache/airflow:2.10.4

# Install linux packages you need:
USER root
RUN apt update && apt install git -y


# Install python packages you need:
USER airflow
RUN pip install json2html networkx py-tes
RUN pip install git+https://github.com/FHIR-Aggregator/submission.git@feature/improve-doc
