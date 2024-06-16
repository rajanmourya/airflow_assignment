# Airflow Project Setup

This project contains Airflow DAGs and the necessary configuration to run them using Docker and Docker Compose.

## Prerequisites

- Docker: https://www.docker.com/products/docker-desktop/ 

## Setup Instructions

### 1. Clone the Repository

First, clone the repository to your local machine:

```sh
git clone https://github.com/rajanmourya/airflow_assignment.git
cd airflow_assignment
```
### 2. Build the Docker Images
```sh
docker-compose build
```
### 3. Initialize the Airflow Database
```sh
docker-compose up airflow-init
```
### 4. Start the Airflow Services
```sh
docker-compose up -d
```

### 5. Check the Status of the Services
```sh
docker-compose ps
```
### 6. Access the Airflow Webserver
* http://localhost:8080

### Info
## Directory Structure
```sh
dags/: Directory containing your DAG files.
plugins/: Directory for custom plugins.
data/: Directory for data files.
logs/: Directory for log files (can be empty).
Dockerfile: Dockerfile for building the Airflow image.
requirements.txt: Python dependencies.
docker-compose.yml: Docker Compose file for orchestrating the services.
```
