# Extend the official Airflow image with the libraries our pipeline needs.
# We rebuild only when requirements.txt changes, so the layer is cached.
FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
