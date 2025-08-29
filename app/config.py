import os

class Config:
    DEBUG = os.getenv('DEBUG')
    if DEBUG is None:
        raise ValueError("Environment variable DEBUG is required")

    API_KEY = os.getenv('API_KEY')
    if API_KEY is None:
        raise ValueError("Environment variable API_KEY is required")

    # Customer Atribute
    CUSTOMER = os.getenv('CUSTOMER')
    if CUSTOMER is None:
        raise ValueError("Environment variable CUSTOMER is required")

    # Domain Atribute
    DOMAIN = os.getenv('DOMAIN')
    if DOMAIN is None:
        raise ValueError("Environment variable DOMAIN is required")

    # Main site
    DBT_INIT_API_LINK = os.getenv('DBT_INIT_API_LINK')
    if DBT_INIT_API_LINK is None:
        raise ValueError("Environment variable DBT_INIT_API_LINK is required")
    
    DATA_MODEL_REPO_URL = os.getenv('DATA_MODEL_REPO_URL')
    if DATA_MODEL_REPO_URL is None:
        raise ValueError("Environment variable DATA_MODEL_REPO_URL is required")

    GROUP_ACCESS_TOKEN = os.getenv('GROUP_ACCESS_TOKEN')
    if GROUP_ACCESS_TOKEN is None:
        raise ValueError("Environment variable GROUP_ACCESS_TOKEN is required")

    MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
    if MINIO_BUCKET_NAME is None:
        raise ValueError("Environment variable MINIO_BUCKET_NAME is required")
    
    MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
    if MINIO_ENDPOINT is None:
        raise ValueError("Environment variable MINIO_ENDPOINT is required")

    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    if MINIO_ACCESS_KEY is None:
        raise ValueError("Environment variable MINIO_ACCESS_KEY is required")
    
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
    if MINIO_SECRET_KEY is None:
        raise ValueError("Environment variable MINIO_SECRET_KEY is required")
    
    # Retrieve environment variables for Data Orchestrator
    DATA_ORCHESTRATOR_BASE_URL = os.getenv('DATA_ORCHESTRATOR_BASE_URL')
    if DATA_ORCHESTRATOR_BASE_URL is None:
        raise ValueError("Environment variable DATA_ORCHESTRATOR_BASE_URL is required")

    DATA_ORCHESTRATOR_BASE_USER = os.getenv('DATA_ORCHESTRATOR_BASE_USER')
    if DATA_ORCHESTRATOR_BASE_USER is None:
        raise ValueError("Environment variable DATA_ORCHESTRATOR_BASE_USER is required")

    DATA_ORCHESTRATOR_BASE_USER_PASSWORD = os.getenv('DATA_ORCHESTRATOR_BASE_USER_PASSWORD')
    if DATA_ORCHESTRATOR_BASE_USER_PASSWORD is None:
        raise ValueError("Environment variable DATA_ORCHESTRATOR_BASE_USER_PASSWORD is required")
    
    DATA_ORCHESTRATOR_REPO_URL = os.getenv('DATA_ORCHESTRATOR_REPO_URL')
    if DATA_ORCHESTRATOR_REPO_URL is None:
        raise ValueError("Environment variable DATA_ORCHESTRATOR_REPO_URL is required")

    # Retrieve environment variables for Data Quality Endpoint
    DC_DQ_ENDPOINT_URL = os.getenv('DC_DQ_ENDPOINT_URL')
    if DC_DQ_ENDPOINT_URL is None:
        raise ValueError("Environment variable DC_DQ_ENDPOINT_URL is required")
    
    DC_DQ_BEARER_TOKEN = os.getenv('DC_DQ_BEARER_TOKEN')
    if DC_DQ_BEARER_TOKEN is None:
        raise ValueError("Environment variable DC_DQ_BEARER_TOKEN is required")

    # # Mail server configuration - Not Used
    # MAIL_SERVER = os.getenv('MAIL_SERVER')
    # if MAIL_SERVER is None:
    #     raise ValueError("Environment variable MAIL_SERVER is required")

    # MAIL_PORT = os.getenv('MAIL_PORT')
    # if MAIL_PORT is None:
    #     raise ValueError("Environment variable MAIL_PORT is required")
    # MAIL_PORT = int(MAIL_PORT)

    # MAIL_USE_TLS = os.getenv('MAIL_USE_TLS')
    # if MAIL_USE_TLS is None:
    #     raise ValueError("Environment variable MAIL_USE_TLS is required")
    # MAIL_USE_TLS = MAIL_USE_TLS == 'True'

    # MAIL_USE_SSL = os.getenv('MAIL_USE_SSL')
    # if MAIL_USE_SSL is None:
    #     raise ValueError("Environment variable MAIL_USE_SSL is required")
    # MAIL_USE_SSL = MAIL_USE_SSL == 'True'

    # MAIL_USERNAME = os.getenv('MAIL_USERNAME')
    # if MAIL_USERNAME is None:
    #     raise ValueError("Environment variable MAIL_USERNAME is required")

    # MAIL_PASSWORD = os.getenv('MAIL_PASSWORD')
    # if MAIL_PASSWORD is None:
    #     raise ValueError("Environment variable MAIL_PASSWORD is required")

    # MAIL_DEFAULT_SENDER = os.getenv('MAIL_DEFAULT_SENDER')
    # if MAIL_DEFAULT_SENDER is None:
    #     raise ValueError("Environment variable MAIL_DEFAULT_SENDER is required")

    # # Database configuration - Not Used
    # DB_HOST = os.getenv('DB_HOST')
    # if DB_HOST is None:
    #     raise ValueError("Environment variable DB_HOST is required")

    # DB_PORT = os.getenv('DB_PORT')
    # if DB_PORT is None:
    #     raise ValueError("Environment variable DB_PORT is required")
    # DB_PORT = int(DB_PORT)

    # DB_USER = os.getenv('DB_USER')
    # if DB_USER is None:
    #     raise ValueError("Environment variable DB_USER is required")

    # DB_PASSWORD = os.getenv('DB_PASSWORD')
    # if DB_PASSWORD is None:
    #     raise ValueError("Environment variable DB_PASSWORD is required")

    # DB_NAME = os.getenv('DB_NAME')
    # if DB_NAME is None:
    #     raise ValueError("Environment variable DB_NAME is required")
