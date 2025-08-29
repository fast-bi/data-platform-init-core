# gunicorn_config.py

import multiprocessing
import os
from config import Config  # make sure to import Config correctly based on your project structure

# Server socket configurations
bind = "0.0.0.0:8888"
backlog = 2048  # Maximum number of pending connections

# Worker processes
workers = multiprocessing.cpu_count() * 2 + 1  # Recommended formula for CPU-bound applications
worker_class = 'sync'  # Use 'gevent' for async support if needed
worker_connections = 1000
timeout = 600  # 10 minutes for long-running jobs
graceful_timeout = 120  # 2 minutes graceful shutdown
keepalive = 5  # How long to wait for requests on a Keep-Alive connection

# Process naming
proc_name = 'dbt-init-api'
pythonpath = '/app'

# SSL Configuration (if using HTTPS)
# keyfile = '/path/to/keyfile'
# certfile = '/path/to/certfile'
# ssl_version = 'TLS'

# Security configurations
limit_request_line = 4096
limit_request_fields = 100
limit_request_field_size = 8190
max_requests = 1000  # Restart workers after this many requests
max_requests_jitter = 50  # Add randomness to max_requests

# Logging
accesslog = '-'  # stdout
errorlog = '-'   # stderr
loglevel = 'info'
access_log_format = '%({x-forwarded-for}i)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Development specific settings
reload = False  # Set to True in development
preload_app = True

# Rate limiting and DoS prevention
worker_max_requests = 1000
worker_max_requests_jitter = 50

def when_ready(server):
    """Run actions when server starts."""
    pass

def on_starting(server):
    """Run actions before the first process is forked."""
    pass

def post_fork(server, worker):
    """Run actions after a worker is forked."""
    server.log.info("Worker spawned (pid: %s)", worker.pid)

def worker_abort(worker):
    """Run actions when a worker times out."""
    worker.log.info("Worker received SIGABRT signal")

# Error handling
def worker_exit(server, worker):
    """Run actions after a worker exits."""
    server.log.info("Worker exited (pid: %s)", worker.pid)

# Custom settings for your application
raw_env = [
    f"CUSTOMER={Config.CUSTOMER}",
    f"DEBUG={Config.DEBUG}",
    f"API_KEY={Config.API_KEY}"
]
