# Use an official Python runtime as a parent image
FROM python:3.11

# Set the working directory in the container to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Copy the entrypoint script into the Docker image and change its permissions
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Set the entrypoint script as the entrypoint for the Docker container
ENTRYPOINT ["/app/entrypoint.sh"]

# Install any needed packages specified in requirements.txt
RUN pip install dbt-snowflake

# Run dbt deps command
RUN dbt deps
