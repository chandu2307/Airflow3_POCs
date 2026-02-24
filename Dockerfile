FROM apache/airflow:3.1.6

USER ${AIRFLOW_UID}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
