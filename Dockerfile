FROM apache/airflow:2.3.0-python3.9
COPY requirements.txt .
RUN pip install -r requirements.txt