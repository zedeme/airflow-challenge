# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Expose the port for Airflow web server
EXPOSE 8080

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/app/airflow
ENV PYTHONPATH="${PYTHONPATH}:${AIRFLOW_HOME}"

# Initialize Airflow database
RUN airflow db init

# Start the Airflow scheduler and webserver
CMD ["airflow", "scheduler", "&", "airflow", "webserver"]