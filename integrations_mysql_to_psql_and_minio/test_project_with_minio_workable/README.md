# tup for Spark + MinIO with Docker
=================================================
This guide provides instructions for setting up a Spark environment with MinIO using Docker.
It assumes you have Docker installed on your machine.

## Docker Compose Setup
-------------------------------------------------   
1. cd to the project directory:
   ```bash
   cd test_project_with_minio
   ```
2. Start the Docker containers using Docker Compose:
   ```bash
    docker-compose up -d
    ```
3. Verify that the containers are running:
   ```bash
    docker-compose ps
    ```
4. minio UI
http://localhost:9001/
5. Spark UI
http://localhost:8080/
6. Access the Jupyterlab:
   ```bash
   http://localhost:8888/
   ```
   You can use the token provided in the logs or set a password in the `docker-compose.yml` file.
-------------------------------------------------   

