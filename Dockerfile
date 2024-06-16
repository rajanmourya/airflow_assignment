# Use the official Airflow image
FROM apache/airflow:2.5.1

# Copy the requirements file if you have any Python dependencies
COPY requirements.txt /requirements.txt

# Install any Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy your DAGs to the appropriate directory
COPY dags /opt/airflow/dags

# Copy the configuration files (if any)
COPY airflow.cfg /opt/airflow/airflow.cfg

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Initialize the database
RUN airflow db init

# Expose the port for the Airflow webserver
EXPOSE 8080

# Set the entrypoint
ENTRYPOINT ["tini", "--"]

# Start the scheduler and webserver
CMD ["airflow", "scheduler", "&", "airflow", "webserver"]
