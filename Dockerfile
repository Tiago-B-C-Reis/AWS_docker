# Use the official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container (this location is inside the docker)
WORKDIR /usr/scr/app

# Install AWS CLI v1
RUN pip install awscli==1.* --no-cache-dir

# Verify the installation
RUN aws --version