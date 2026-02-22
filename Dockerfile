# Use an official, lightweight Python image
FROM python:3.10-slim

# Install system dependencies required for cron and dbt
RUN apt-get update && \
    apt-get install -y cron git && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create a log file for cron jobs later
RUN touch /var/log/cron.log

# Command to keep the container alive so we can develop inside it
CMD ["tail", "-f", "/dev/null"]