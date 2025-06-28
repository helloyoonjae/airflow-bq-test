FROM apache/airflow:3.0.2

# GCP provider 설치
RUN pip install --no-cache-dir apache-airflow-providers-google
