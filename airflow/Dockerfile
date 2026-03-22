# Start from official Airflow image
FROM apache/airflow:2.9.0-python3.12

# Switch to root to install system packages if needed
USER root

# Create necessary directories
RUN mkdir -p /opt/airflow/src /opt/airflow/data /opt/airflow/stock_dbt

# Switch back to airflow user for pip installs
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set working directory
WORKDIR /opt/airflow