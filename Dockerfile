# Top level build args
ARG build_for=linux/amd64

##
# base image (abstract)
##
FROM --platform=$build_for python:3.11.11-slim-bullseye AS base
LABEL maintainer=support@fast.bi

# Install system dependencies and clean up in a single layer
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    git \
    curl \
    && mkdir -p /etc/apt/keyrings \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update \
    && apt-get install google-cloud-sdk -y \
    && apt-get upgrade -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Create all required directories
RUN mkdir -p /usr/src/app \
    /usr/src/app/init_dbt_project_files/ \
    /usr/src/app/init_setup_files_v1/ \
    /usr/src/app/init_setup_files_v2/

WORKDIR /usr/src/app

# Copy and install requirements first (this layer won't change unless requirements.txt changes)
COPY requirements.txt /usr/src/app/
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

# Create symlinks
RUN ln -s /usr/src/app/init_dbt_project_files /init_dbt_project_files && \
    ln -s /usr/src/app/init_setup_files_v1 /init_setup_files_v1 && \
    ln -s /usr/src/app/init_setup_files_v2 /init_setup_files_v2

# Copy configuration files first (smaller and changed less frequently)
COPY init_dbt_project_files/ /usr/src/app/init_dbt_project_files/
COPY init_setup_files_v1/ /usr/src/app/init_setup_files_v1/
COPY init_setup_files_v2/ /usr/src/app/init_setup_files_v2/

# Copy application code last (most likely to change)
COPY app/ /usr/src/app/

EXPOSE 8888

ENTRYPOINT ["/bin/sh", "-c", "gunicorn --config /usr/src/app/gunicorn_config.py wsgi:app"]