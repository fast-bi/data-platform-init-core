import os
import datetime
from apiflask import APIFlask, Schema, abort, APIBlueprint
from apiflask.fields import Integer, String, Boolean, URL, DateTime, Raw, List
from apiflask.validators import Length, OneOf, ValidationError, Equal
from flask import request, jsonify
from security import auth
import json
import random
import re
import ruamel.yaml
import git
import shutil
import string
from jinja2 import Template
import re

# List of predefined schedules
PREDEFINED_SCHEDULES = {"@once", "@hourly", "@daily", "@weekly", "@monthly", "@yearly"}

# Regular expression for validating cron expressions
CRON_REGEX = r"^(\*|([0-5]?[0-9])) (\*|([0-5]?[0-9])) (\*|([0-2]?[0-9])) (\*|([0-3]?[0-9])) (\*|([0-1]?[0-9])) (\*|([0-6]))$"

def validate_schedule(value):
    if value in PREDEFINED_SCHEDULES:
        return True
    if re.match(CRON_REGEX, value):
        return True
    raise ValidationError("Invalid schedule format. Use a valid cron expression or one of the predefined schedules.")


class K8SInputSchema(Schema):
    init_version = String(required=False, load_default='2',metadata={'title': 'Project init version','description': '1 - Denormalized raw tables, 2 - Normalized raw tables','example': '2'})
    dbt_project_name = String(required=True, validate=Length(1, 255), metadata={'title': 'DBT Project Name', 'description': 'The name of the new DBT project.', 'example': "fastbi_demo_dbt_project"})
    dbt_project_owner = String(required=True, validate=Length(1, 255), metadata={'title': 'DBT Project Owner', 'description': 'DBT Project Owner name.', 'example': "Fast.bi"})

    gcp_project_id = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP Project ID', 'description': 'The ID of the Google Cloud Project.', 'example': "fast-bi-demo"})
    gcp_project_region = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP Project region', 'description': 'The geographic location where your Google Cloud (BigQuery) resources are hosted, like europe-central2 or us-central1', 'example': "europe-central2"})
    gcp_project_sa_email = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP SA email', 'description': 'The Service Account email used to authenticate and authorize access to Google Cloud resources', 'example': "dbt-sa@your-project-id.iam.gserviceaccount.com"})

    snowflake_db = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    snowflake_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})
    snowflake_warehouse = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake warehouse', 'description': 'The compute resource that runs your SQL queries. Warehouses scale separately from the data storage.', 'example': "COMPUTE_WH"})
    snowflake_role = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake Role', 'description': 'The permission set assigned to a user or service account that defines what actions they are allowed to perform in the database or cloud environment.', 'example': "ACCOUNTADMIN"})

    redshift_database = String(required=False, validate=Length(1, 255), metadata={'title': 'Redshift database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    redshift_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Redshift schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})

    fabric_database = String(required=False, validate=Length(1, 255), metadata={'title': 'Fabric database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    fabric_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Fabric schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})

    project_level = String(required=True, validate=OneOf(['DEV', 'TEST', 'QA', 'UAT', 'EI', 'PRE', 'STG', 'NON_PROD', 'PROD', 'CORP', 'RVW']), metadata={'title': 'Project Level', 'description': 'The Data Governance Fabric group type.', 'example': 'PROD'})
    workload_platform = String(required=True, validate=OneOf(['Airflow', 'Composer']), metadata={'title': 'Workload Platform', 'description': 'The Data Orchestration service.', 'example': 'Airflow'})
    platform_namespace = String(required=False, validate=Length(1, 255), load_default="data-orchestration", dump_default="data-orchestration", metadata={'title': 'Namespace Name', 'description': 'Kubernetes Cluster dbt project workload pipeline namespace name.', 'example': 'data-orchestration'})
    operator = String(required=False, validate=Equal('k8s'), load_default='k8s', dump_default='k8s', metadata={'title': 'Operator', 'description': 'The operator responsible for the DBT project.', 'example': 'k8s'})
    dbt_k8s_pod_name = String(required=False, validate=Length(1, 68), load_default='dbt-k8s', dump_default='dbt-k8s', metadata={'title': 'DBT K8S Operator Pod Name', 'description': 'The name of the Kubernetes pod for the DBT project.', 'example': 'dbt-k8s'})
    data_model_repository_url = String(required=False, metadata={'title': 'Data Model Repository URL', 'description': 'The URL of the data model repository.', 'example': 'https://gitlab.fast.bi/bi-platform/common/data-models/dbt-data-models.git'})
    dbt_dag_schedule_interval = String(required=False, validate=validate_schedule, metadata={'title': 'DBT DAG Schedule Interval', 'description': 'The schedule interval for the DBT DAG. Can be a cron expression or a predefined schedule like @once, @daily, etc.', 'example': '@daily'})
    dbt_dag_tag = String(required=False, validate=Length(1, 255), load_default='PROD', dump_default='PROD', metadata={'title': 'DBT Project DAG Tag', 'description': 'The tag for the DBT Project DAG.', 'example': 'DEV'})
    tsb_dbt_core_image_version = String(required=False, load_default='4fastbi/dbt-workflow-core:latest', dump_default='4fastbi/dbt-workflow-core:latest', metadata={'title': 'TSB DBT Core Image Version', 'description': 'The version of the TSB DBT core image.', 'example': '4fastbi/dbt-workflow-core:latest'})
    dbt_seed_enabled = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'DBT Seed Enabled', 'description': 'Flag to enable DBT seed.', 'example': False})
    dbt_seed_sharding_enabled = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'DBT Seed Sharding Enabled', 'description': 'Flag to enable DBT seed sharding.', 'example': False})
    dbt_source_enabled = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'DBT Source Freshness Enabled', 'description': 'A boolean value indicating whether source dataset and source freshness verification is enabled (True) or not (False).', 'example': False})
    dbt_snapshot_enabled = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'DBT Snapshot Enabled', 'description': 'Flag to enable DBT snapshot.', 'example': False})
    dbt_snapshot_sharding_enabled = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'DBT Snapshot Sharding Enabled', 'description': 'Flag to enable DBT snapshot sharding.', 'example': False})
    dbt_debug = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'DBT Debug', 'description': 'Flag to enable DBT debug mode.', 'example': False})
    dbt_model_debug_log = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'DBT Model Debug Log', 'description': 'Flag to enable DBT model debug logging.', 'example': False})
    data_quality_enabled = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'Data Quality Enabled', 'description': 'Flag to enable data quality checks.', 'example': False})
    datahub_enabled = Boolean(required=False, load_default=True, dump_default=True, metadata={'title': 'DataHub Enabled', 'description': 'Flag to enable DataHub integration.', 'example': True})
    data_analysis_dbt_project_name = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'Data-Analysis DBT Project Name', 'description': 'Flag to enable Data Analysis for DBT project.', 'example': False})
    airbyte_workspace_id = String(required=False, metadata={'title': 'Airbyte Workspace ID', 'description': 'The Airbyte workspace ID.', 'example': ""})
    airbyte_connection_id = List(String(), required=False, metadata={'title': 'Airbyte Connection IDs', 'description': 'The Airbyte connection IDs.', 'example': ""})
    airbyte_connection_name = String(required=False, metadata={'title': 'Airbyte Connection Name', 'description': 'The Airbyte connection name.', 'example': ""})
    airbyte_replication_flag = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'Airbyte replication flag', 'description': 'Flag to create airbyte_group or not.', 'example': False})
    airbyte_destination_name = String(required=False, metadata={'title': 'Airbyte Destination name', 'description': 'DWH DB destination name. Need to build profile.yml', 'example': ""})
    data_warehouse_platform = String(required=False, metadata={'title': 'Airbyte Destination type', 'description': 'DWH DB destination type. bigquery, snowflake', 'example': ''})
    branch_name = String(required=True, metadata={'title': 'Branch Name', 'description': 'The Git branch name for the DBT project.', 'example': 'DD_00000001_DEMO'})
    advanced_properties = String(required=False, metadata={'title': 'Advanced Properties', 'description': 'The form data for the advanced properties section.', 'example': ""})
    recaptchaResponse = String(required=False, metadata={'title': 'System Recaptcha Response', 'description': 'System', 'example': ""})
    reinit_project = Boolean(required=False, load_default=False, dump_default=False, metadata={'title': 'Initialize models for project', 'description': 'Flag to enable extra logic in initialization process. Init models over project', 'example': False})


class K8SOutputSchema(Schema):
    output = Raw(metadata={'description': 'The Output of the DBT project k8s-operator initialization.'})


# Define APIInputSchema
class APIInputSchema(Schema):
    init_version = String(required=False, dump_default='2', metadata={'title': 'Project init version', 'description': '1 - Denormalized raw tables, 2 - Normalized raw tables', 'example': '2'})
    dbt_project_name = String(required=True, validate=Length(1, 255), metadata={'title': 'DBT Project Name', 'description': 'The name of the new DBT project.', 'example': "fastbi_demo_dbt_project"})
    dbt_project_owner = String(required=True, validate=Length(1, 255), metadata={'title': 'DBT Project Owner', 'description': 'DBT Project Owner name.', 'example': "Fast.bi"})

    gcp_project_id = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP Project ID', 'description': 'The ID of the Google Cloud Project.', 'example': "fast-bi-demo"})
    gcp_project_region = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP Project region', 'description': 'The geographic location where your Google Cloud (BigQuery) resources are hosted, like europe-central2 or us-central1', 'example': "europe-central2"})
    gcp_project_sa_email = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP SA email', 'description': 'The Service Account email used to authenticate and authorize access to Google Cloud resources', 'example': "dbt-sa@your-project-id.iam.gserviceaccount.com"})

    snowflake_db = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    snowflake_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})
    snowflake_warehouse = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake warehouse', 'description': 'The compute resource that runs your SQL queries. Warehouses scale separately from the data storage.', 'example': "COMPUTE_WH"})
    snowflake_role = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake Role', 'description': 'The permission set assigned to a user or service account that defines what actions they are allowed to perform in the database or cloud environment.', 'example': "ACCOUNTADMIN"})

    redshift_database = String(required=False, validate=Length(1, 255), metadata={'title': 'Redshift database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    redshift_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Redshift schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})

    fabric_database = String(required=False, validate=Length(1, 255), metadata={'title': 'Fabric database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    fabric_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Fabric schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})

    project_level = String(required=True, validate=OneOf(['DEV', 'TEST', 'QA', 'UAT', 'EI', 'PRE', 'STG', 'NON_PROD', 'PROD', 'CORP', 'RVW']), metadata={'title': 'Project Level', 'description': 'The Data Governance Fabric group type.', 'example': 'PROD'})
    workload_platform = String(required=True, validate=OneOf(['Airflow', 'Composer']), metadata={'title': 'Workload Platform', 'description': 'The Data Orchestration service.', 'example': 'Airflow'})
    platform_namespace = String(required=False, dump_default="dbt-server", validate=Length(1, 255), metadata={'title': 'Namespace Name', 'description': 'Kubernetes Cluster dbt project workload pipeline namespace name.', 'example': "dbt-server"})
    operator = String(required=False, validate=Equal('api'), dump_default='api', metadata={'title': 'Operator', 'description': 'The operator responsible for the DBT project.', 'example': 'api'})
    dbt_k8s_pod_name = String(required=False, validate=Length(1, 68), dump_default='dbt-api', metadata={'title': 'DBT API Operator Pod Name', 'description': 'The name of the Kubernetes pod for the DBT project.', 'example': 'dbt-api'})
    data_model_repository_url = String(required=False, metadata={'title': 'Data Model Repository URL', 'description': 'The URL of the data model repository.', 'example': 'https://gitlab.fast.bi/bi-platform/common/data-models/dbt-data-models.git'})
    dbt_dag_schedule_interval = String(required=False, validate=validate_schedule, metadata={'title': 'DBT DAG Schedule Interval', 'description': 'The schedule interval for the DBT DAG. Can be a cron expression or a predefined schedule like @once, @daily, etc.', 'example': '@daily'})
    dbt_dag_tag = String(required=False, validate=Length(1, 255), dump_default='PROD', metadata={'title': 'DBT Project DAG Tag', 'description': 'The tag for the DBT Project DAG.', 'example': 'DEV'})
    tsb_dbt_core_image_version = String(required=False, dump_default='4fastbi/dbt-workflow-core:latest', metadata={'title': 'TSB DBT Core Image Version', 'description': 'The version of the TSB DBT core image.', 'example': '4fastbi/dbt-workflow-core:latest'})
    dbt_seed_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Seed Enabled', 'description': 'Flag to enable DBT seed.', 'example': False})
    dbt_seed_sharding_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Seed Sharding Enabled', 'description': 'Flag to enable DBT seed sharding.', 'example': False})
    dbt_source_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Source Freshness Enabled', 'description': 'A boolean value indicating whether source dataset and source freshness verification is enabled (True) or not (False).', 'example': False})
    dbt_snapshot_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Snapshot Enabled', 'description': 'Flag to enable DBT snapshot.', 'example': False})
    dbt_snapshot_sharding_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Snapshot Sharding Enabled', 'description': 'Flag to enable DBT snapshot sharding.', 'example': False})
    dbt_debug = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Debug', 'description': 'Flag to enable DBT debug mode.', 'example': False})
    dbt_model_debug_log = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Model Debug Log', 'description': 'Flag to enable DBT model debug logging.', 'example': False})
    data_quality_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'Data Quality Enabled', 'description': 'Flag to enable data quality checks.', 'example': False})
    datahub_enabled = Boolean(required=False, dump_default=True, metadata={'title': 'DataHub Enabled', 'description': 'Flag to enable DataHub integration.', 'example': True})
    data_analysis_dbt_project_name = Boolean(required=False, dump_default=False, metadata={'title': 'Data-Analysis DBT Project Name', 'description': 'Flag to enable Data Analysis for DBT project.', 'example': False})
    airbyte_workspace_id = String(required=False, metadata={'title': 'Airbyte Workspace ID', 'description': 'The Airbyte workspace ID.', 'example': ""})
    airbyte_connection_id = List(String(), required=False, metadata={'title': 'Airbyte Connection IDs', 'description': 'The Airbyte connection IDs.', 'example': ""})
    airbyte_connection_name = String(required=False, metadata={'title': 'Airbyte Connection Name', 'description': 'The Airbyte connection name.', 'example': ""})
    airbyte_replication_flag = Boolean(required=False, dump_default=False, metadata={'title': 'Airbyte replication flag', 'description': 'Flag to create airbyte_group or not.', 'example': False})
    airbyte_destination_name = String(required=False, metadata={'title': 'Airbyte Destination name', 'description': 'DWH DB destination name. Need to build profile.yml', 'example': ""})
    data_warehouse_platform = String(required=False, metadata={'title': 'Airbyte Destination type', 'description': 'DWH DB destination type. bigquery, snowflake', 'example': ''})
    branch_name = String(required=True, metadata={'title': 'Branch Name', 'description': 'The Git branch name for the DBT project.', 'example': 'DD_00000001_DEMO'})
    advanced_properties = String(required=False, metadata={'title': 'Advanced Properties', 'description': 'The form data for the advanced properties section.', 'example': ""})
    recaptchaResponse = String(required=False, metadata={'title': 'System Recaptcha Response', 'description': 'System', 'example': ""})
    reinit_project = Boolean(required=False, dump_default=False, metadata={'title': 'Initialize models for project', 'description': 'Flag to enable extra logic in initialization process. Init models over project', 'example': False})

class APIOutputSchema(Schema):
    output = Raw(metadata={'description': 'The Output of the DBT project api-operator initialization.'})

# Define APIInputSchema
class BashInputSchema(Schema):
    init_version = String(required=False, dump_default='2', metadata={'title': 'Project init version', 'description': '1 - Denormalized raw tables, 2 - Normalized raw tables', 'example': '2'})
    dbt_project_name = String(required=True, validate=Length(1, 255), metadata={'title': 'DBT Project Name', 'description': 'The name of the new DBT project.', 'example': "fastbi_demo_dbt_project"})
    dbt_project_owner = String(required=True, validate=Length(1, 255), metadata={'title': 'DBT Project Owner', 'description': 'DBT Project Owner name.', 'example': "Fast.bi"})

    gcp_project_id = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP Project ID', 'description': 'The ID of the Google Cloud Project.', 'example': "fast-bi-demo"})
    gcp_project_region = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP Project region', 'description': 'The geographic location where your Google Cloud (BigQuery) resources are hosted, like europe-central2 or us-central1', 'example': "europe-central2"})
    gcp_project_sa_email = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP SA email', 'description': 'The Service Account email used to authenticate and authorize access to Google Cloud resources', 'example': "dbt-sa@your-project-id.iam.gserviceaccount.com"})

    snowflake_db = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    snowflake_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})
    snowflake_warehouse = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake warehouse', 'description': 'The compute resource that runs your SQL queries. Warehouses scale separately from the data storage.', 'example': "COMPUTE_WH"})
    snowflake_role = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake Role', 'description': 'The permission set assigned to a user or service account that defines what actions they are allowed to perform in the database or cloud environment.', 'example': "ACCOUNTADMIN"})

    redshift_database = String(required=False, validate=Length(1, 255), metadata={'title': 'Redshift database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    redshift_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Redshift schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})

    fabric_database = String(required=False, validate=Length(1, 255), metadata={'title': 'Fabric database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    fabric_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Fabric schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})

    project_level = String(required=True, validate=OneOf(['DEV', 'TEST', 'QA', 'UAT', 'EI', 'PRE', 'STG', 'NON_PROD', 'PROD', 'CORP', 'RVW']), dump_default='PROD', metadata={'title': 'Project Level', 'description': 'The Data Governance Fabric group type.', 'example': 'PROD'})
    workload_platform = String(required=True, validate=OneOf(['Airflow', 'Composer']), dump_default='Airflow', metadata={'title': 'Workload Platform', 'description': 'The Data Orchestration service.', 'example': 'Airflow'})
    operator = String(required=False, validate=Equal('bash'), dump_default='bash', metadata={'title': 'Operator', 'description': 'The operator responsible for the DBT project.', 'example': 'bash'})
    data_model_repository_url = String(required=False, metadata={'title': 'Data Model Repository URL', 'description': 'The URL of the data model repository.', 'example': 'https://gitlab.fast.bi/bi-platform/common/data-models/dbt-data-models.git'})
    dbt_dag_schedule_interval = String(required=False, validate=validate_schedule, metadata={'title': 'DBT DAG Schedule Interval', 'description': 'The schedule interval for the DBT DAG. Can be a cron expression or a predefined schedule like @once, @daily, etc.', 'example': '@daily'})
    dbt_dag_tag = String(required=False, validate=Length(1, 255), dump_default='PROD', metadata={'title': 'DBT Project DAG Tag', 'description': 'The tag for the DBT Project DAG.', 'example': 'DEV'})
    dbt_seed_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Seed Enabled', 'description': 'Flag to enable DBT seed.', 'example': False})
    dbt_seed_sharding_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Seed Sharding Enabled', 'description': 'Flag to enable DBT seed sharding.', 'example': False})
    dbt_snapshot_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Snapshot Enabled', 'description': 'Flag to enable DBT snapshot.', 'example': False})
    dbt_snapshot_sharding_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Snapshot Sharding Enabled', 'description': 'Flag to enable DBT snapshot sharding.', 'example': False})
    dbt_source_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Source Freshness Enabled', 'description': 'A boolean value indicating whether source dataset and source freshness verification is enabled (True) or not (False).', 'example': False})
    dbt_debug = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Debug', 'description': 'Flag to enable DBT debug mode.', 'example': False})
    dbt_model_debug_log = Boolean(required=False, dump_default=False, metadata={'title': 'DBT Model Debug Log', 'description': 'Flag to enable DBT model debug logging.', 'example': False})
    data_quality_enabled = Boolean(required=False, dump_default=False, metadata={'title': 'Data Quality Enabled', 'description': 'Flag to enable data quality checks.', 'example': False})
    datahub_enabled = Boolean(required=False, dump_default=True, metadata={'title': 'DataHub Enabled', 'description': 'Flag to enable DataHub integration.', 'example': True})
    data_analysis_dbt_project_name = Boolean(required=False, dump_default=False, metadata={'title': 'Data-Analysis DBT Project Name', 'description': 'Flag to enable Data Analysis for DBT project.', 'example': False})
    airbyte_workspace_id = String(required=False, metadata={'title': 'Airbyte Workspace ID', 'description': 'The Airbyte workspace ID.', 'example': ""})
    airbyte_connection_id = List(String(), required=False, metadata={'title': 'Airbyte Connection IDs', 'description': 'The Airbyte connection IDs.', 'example': ""})
    airbyte_connection_name = String(required=False, metadata={'title': 'Airbyte Connection Name', 'description': 'The Airbyte connection name.', 'example': ""})
    airbyte_replication_flag = Boolean(required=False, dump_default=False, metadata={'title': 'Airbyte replication flag', 'description': 'Flag to create airbyte_group or not.', 'example': False})
    airbyte_destination_name = String(required=False, metadata={'title': 'Airbyte Destination name', 'description': 'DWH DB destination name. Need to build profile.yml', 'example': ""})
    data_warehouse_platform = String(required=False, metadata={'title': 'Airbyte Destination type', 'description': 'DWH DB destination type. bigquery, snowflake', 'example': ''})
    branch_name = String(required=True, metadata={'title': 'Branch Name', 'description': 'The Git branch name for the DBT project.', 'example': 'DD_00000001_DEMO'})
    lightdash_dbt_project_name = String(required=False, metadata={'title': 'Deprecated - Data-Analysis DBT Project Name', 'description': 'The Lightdash DBT project name.', 'example': ""})
    recaptchaResponse = String(required=False, metadata={'title': 'System Recaptcha Response', 'description': 'System', 'example': ""})
    reinit_project = Boolean(required=False, dump_default=False, metadata={'title': 'Initialize models for project', 'description': 'Flag to enable extra logic in initialization process. Init models over project', 'example': False})

class BashOutputSchema(Schema):
    output = Raw(metadata={'description': 'The Output of the DBT project bash-operator initialization.'})

# Define GKEInputSchema
class GKEInputSchema(Schema):
    init_version = String(required=False, load_default='2', metadata={'example': '2', 'title': 'Project init version', 'description': '1 - Denormalized raw tables, 2 - Normalized raw tables'})
    cluster_name = String(required=True, validate=Length(1, 255), metadata={'example': 'dbt-fast-bi-workload', 'title': 'Cluster Name', 'description': 'The name of the GKE cluster.'})
    cluster_zone = String(required=True, validate=Length(1, 255), metadata={'example': 'europe-central2', 'title': 'Cluster Zone', 'description': 'The zone of the GKE cluster.'})
    cluster_node_count = String(required=True, validate=Length(1, 255), metadata={'example': '3', 'title': 'Cluster Node Count', 'description': 'The number of nodes in the GKE cluster.'})
    cluster_machine_type = String(required=True, validate=Length(1, 255), metadata={'example': 'e2-standard-4', 'title': 'Cluster Machine Type', 'description': 'The machine type of the GKE cluster nodes.'})
    cluster_machine_disk_type = String(required=True, validate=Length(1, 255), metadata={'example': 'pd-balanced', 'title': 'Cluster Machine Disk Type', 'description': 'The disk type of the GKE cluster nodes.'})
    network = String(required=True, validate=Length(1, 255), metadata={'example': 'projects/shared-vpc-project-id/global/networks/vpc-name', 'title': 'Network', 'description': 'The network configuration for the GKE cluster.'})
    subnetwork = String(required=True, validate=Length(1, 255), metadata={'example': 'projects/shared-vpc-project-id/regions/europe-central2/subnetworks/vpc-subnetwork-name', 'title': 'Subnetwork', 'description': 'The subnetwork configuration for the GKE cluster.'})
    privatenodes_ip_range = String(required=True, validate=Length(1, 255), metadata={'example': '10.201.97.128/28', 'title': 'Private Nodes IP Range', 'description': 'The IP range for private nodes in the GKE cluster.'})
    shared_vpc = Boolean(required=True, metadata={'example': False, 'title': 'Shared VPC', 'description': 'Whether the GKE cluster uses a shared VPC.'})
    services_secondary_range_name = String(required=True, validate=Length(1, 255), metadata={'example': 'cluster-secondary-subnetwork-name', 'title': 'Services Secondary Range Name', 'description': 'The secondary range name for services in the GKE cluster.'})
    cluster_secondary_range_name = String(required=True, validate=Length(1, 255), metadata={'example':'services-secondary-subnetwork-name', 'title': 'Cluster Secondary Range Name', 'description': 'The secondary range name for the GKE cluster.'})
    workload_platform = String(required=True, validate=OneOf(['Airflow', 'Composer']), metadata={'example': 'Airflow', 'title': 'Workload Platform', 'description': 'The data orchestration service.'})
    platform_namespace = String(required=False, validate=Length(1, 255), load_default="default", metadata={'example': 'default', 'title': 'Platform Namespace', 'description': 'The Kubernetes namespace for the workload.'})
    operator = String(required=False, validate=Equal('gke'), load_default="gke", metadata={'example':'gke', 'title': 'Operator', 'description': 'The operator responsible for the DBT project.'})
    dbt_k8s_pod_name = String(required=False, validate=Length(1, 255), load_default="dbt-gke", metadata={'example': 'dbt-gke', 'title': 'DBT K8S Pod Name', 'description': 'The name of the Kubernetes pod for the DBT project.'})
    data_model_repository_url = String(required=False, validate=Length(1, 255), load_default="https://gitlab.fast.bi/bi-platform/common/data-models/dbt-data-models.git", metadata={'example': 'https://gitlab.fast.bi/bi-platform/common/data-models/dbt-data-models.git', 'title': 'Data Model Repository URL', 'description': 'The URL of the data model repository.'})
    dbt_project_name = String(required=True, validate=Length(1, 255), metadata={'example': 'fastbi_demo_dbt_project', 'title': 'DBT Project Name', 'description': 'The name of the DBT project.'})
    dbt_project_owner = String(required=True, validate=Length(1, 255), metadata={'example': 'Fast.bi', 'title': 'DBT Project Owner', 'description': 'DBT Project Owner name.'})
    dbt_dag_schedule_interval = String(required=False, validate=validate_schedule, metadata={'title': 'DBT DAG Schedule Interval', 'description': 'The schedule interval for the DBT DAG.', 'example': '@daily'})
    dbt_dag_tag = String(required=False, validate=Length(1, 255), load_default='PROD', metadata={'title': 'DBT DAG Tag', 'description': 'The tag for the DBT DAG.', 'example': 'DEV'})

    gcp_project_id = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP Project ID', 'description': 'The ID of the Google Cloud Project.', 'example': "fast-bi-demo"})
    gcp_project_region = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP Project region', 'description': 'The geographic location where your Google Cloud (BigQuery) resources are hosted, like europe-central2 or us-central1', 'example': "europe-central2"})
    gcp_project_sa_email = String(required=False, validate=Length(1, 255), metadata={'title': 'GCP SA email', 'description': 'The Service Account email used to authenticate and authorize access to Google Cloud resources', 'example': "dbt-sa@your-project-id.iam.gserviceaccount.com"})

    snowflake_db = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    snowflake_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})
    snowflake_warehouse = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake warehouse', 'description': 'The compute resource that runs your SQL queries. Warehouses scale separately from the data storage.', 'example': "COMPUTE_WH"})
    snowflake_role = String(required=False, validate=Length(1, 255), metadata={'title': 'Snowflake Role', 'description': 'The permission set assigned to a user or service account that defines what actions they are allowed to perform in the database or cloud environment.', 'example': "ACCOUNTADMIN"})

    redshift_database = String(required=False, validate=Length(1, 255), metadata={'title': 'Redshift database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    redshift_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Redshift schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})

    fabric_database = String(required=False, validate=Length(1, 255), metadata={'title': 'Fabric database', 'description': 'The database name where your data will be stored or queried from.', 'example': "FASTBI"})
    fabric_schema = String(required=False, validate=Length(1, 255), metadata={'title': 'Fabric schema', 'description': 'A logical grouping of tables within a database. Helps organize your data into different namespaces.', 'example': "PUBLIC"})

    project_level = String(required=True, validate=OneOf(['DEV', 'TEST', 'QA', 'UAT', 'EI', 'PRE', 'STG', 'NON_PROD', 'PROD', 'CORP', 'RVW']), metadata={'title': 'Project Level', 'description': 'The Data Governance Fabric group type.', 'example': 'PROD'})
    tsb_dbt_core_image_version = String(required=False, validate=Length(1, 255), load_default="4fastbi/dbt-workflow-core:latest", metadata={'title': 'TSB DBT Core Image Version', 'description': 'The version of the TSB DBT core image.', 'example': '4fastbi/dbt-workflow-core:latest'})
    dbt_seed_enabled = Boolean(required=False, load_default=False, metadata={'title': 'DBT Seed Enabled', 'description': 'Flag to enable DBT seed.', 'example': False})
    dbt_seed_sharding_enabled = Boolean(required=False, load_default=False, metadata={'title': 'DBT Seed Sharding Enabled', 'description': 'Flag to enable DBT seed sharding.', 'example': False})
    dbt_source_enabled = Boolean(required=False, load_default=False, metadata={'title': 'DBT Source Freshness Enabled', 'description': 'A boolean value indicating whether source dataset and source freshness verification is enabled (True) or not (False).', 'example': False})
    dbt_snapshot_enabled = Boolean(required=False, load_default=False, metadata={'title': 'DBT Snapshot Enabled', 'description': 'Flag to enable DBT snapshot.', 'example': False})
    dbt_snapshot_sharding_enabled = Boolean(required=False, load_default=False, metadata={'title': 'DBT Snapshot Sharding Enabled', 'description': 'Flag to enable DBT snapshot sharding.', 'example': False})
    dbt_debug = Boolean(required=False, load_default=False, metadata={'title': 'DBT Debug', 'description': 'Flag to enable DBT debug mode.', 'example': False})
    dbt_model_debug_log = Boolean(required=False, load_default=False, metadata={'title': 'DBT Model Debug Log', 'description': 'Flag to enable DBT model debug logging.', 'example': False})
    data_quality_enabled = Boolean(required=False, load_default=False, metadata={'title': 'Data Quality Enabled', 'description': 'Flag to enable data quality checks.', 'example': False})
    datahub_enabled = Boolean(required=False, load_default=True, metadata={'title': 'DataHub Enabled', 'description': 'Flag to enable DataHub integration.', 'example': True})
    data_analysis_dbt_project_name = Boolean(required=False, load_default=False, metadata={'title': 'Data-Analysis DBT Project Name', 'description': 'Flag to enable Data Analysis for DBT project.', 'example': False})
    airbyte_workspace_id = String(required=False, metadata={'title': 'Airbyte Workspace ID', 'description': 'The Airbyte workspace ID.', 'example': ''})
    airbyte_connection_id = List(String(), required=False, metadata={'title': 'Airbyte Connection IDs', 'description': 'The Airbyte connection IDs.', 'example': ""})
    airbyte_connection_name = String(required=False, metadata={'title': 'Airbyte Connection Name', 'description': 'The Airbyte connection name.', 'example': ''})
    airbyte_replication_flag = Boolean(required=False, load_default=False, metadata={'title': 'Airbyte replication flag', 'description': 'Flag to create airbyte_group or not.', 'example': False})
    airbyte_destination_name = String(required=False, metadata={'title': 'Airbyte Destination name', 'description': 'DWH DB destination name. Need to build profile.yml', 'example': ''})
    data_warehouse_platform = String(required=False, metadata={'title': 'Airbyte Destination type', 'description': 'DWH DB destination type. bigquery, snowflake', 'example': ''})
    branch_name = String(required=True, metadata={'title': 'Branch Name', 'description': 'The Git branch name for the DBT project.', 'example': 'DD_00000001_DEMO'})
    advanced_properties = String(required=False, metadata={'title': 'Advanced Properties', 'description': 'The form data for the advanced properties section.', 'example': ''})
    recaptchaResponse = String(required=False, metadata={'title': 'System Recaptcha Response', 'description': 'System', 'example': ''})
    reinit_project = Boolean(required=False, load_default=False, metadata={'title': 'Initialize models for project', 'description': 'Flag to enable extra logic in initialization process. Init models over project', 'example': False})


class GKEOutputSchema(Schema):
    output = Raw(metadata={'description': 'The Output of the DBT project gke-operator initialization.'})

def merge_directories(source_path, destination_path):
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source path does not exist: {source_path}")

    if not os.path.exists(destination_path):
        os.makedirs(destination_path)

    for item in os.listdir(source_path):
        source_item = os.path.join(source_path, item)
        destination_item = os.path.join(destination_path, item)

        if os.path.isdir(source_item):
            # Recursively merge directories
            merge_directories(source_item, destination_item)
        else:
            # Copy file, overwrite if it exists
            shutil.copy2(source_item, destination_item)


# Define a global function to handle the common logic
def create_dbt_project(data, request_data, env):
    try:
        # Get warehouse type from data
        warehouse_type = data.get('data_warehouse_platform', '').lower()
        
        # Read secrets based on warehouse type
        secrets = {}
        if warehouse_type:
            secret_base_path = f"/fastbi/secrets/{warehouse_type}"
            if os.path.exists(secret_base_path):
                # Define secret mappings for each warehouse type
                secret_mappings = {
                    'snowflake': [
                        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_USER',
                        'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_PASSWORD'
                    ],
                    'redshift': [
                        'REDSHIFT_PASSWORD', 'REDSHIFT_USER', 'REDSHIFT_HOST',
                        'REDSHIFT_PORT', 'REDSHIFT_DATABASE'
                    ],
                    'fabric': [
                        'FABRIC_USER', 'FABRIC_PASSWORD', 'FABRIC_SERVER',
                        'FABRIC_DATABASE', 'FABRIC_PORT', 'FABRIC_AUTHENTICATION'
                    ]
                }
                
                # Read secrets for the warehouse type
                if warehouse_type in secret_mappings:
                    for secret_name in secret_mappings[warehouse_type]:
                        secret_path = os.path.join(secret_base_path, secret_name)
                        if os.path.exists(secret_path):
                            with open(secret_path, 'r') as f:
                                secrets[secret_name.lower()] = f.read().strip()
        
        # Merge secrets into data dictionary
        data.update(secrets)

        # Generate a unique filename based on datetime and a random number
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        random_number = random.randint(1000, 99999)
        data_file_path = f"request_data_{timestamp}_{random_number}.txt"
        
        # Save the form data to the file
        with open(data_file_path, "a") as file:
            json.dump(request_data, file, indent=4)

        # Load the YAML template
        ## Load the Airflow Variables YAML template
        if env == "k8s":
            airflow_variables_template_file = (
                "/init_dbt_project_files/k8s_dbt_airflow_variables.yml"
            )
        elif env == "gke":
            airflow_variables_template_file = (
                "/init_dbt_project_files/gke_dbt_airflow_variables.yml"
            )
        elif env == "api":
            airflow_variables_template_file = (
                "/init_dbt_project_files/api_dbt_airflow_variables.yml"
            )
        elif env == "bash":
            airflow_variables_template_file = (
            "/init_dbt_project_files/bash_dbt_airflow_variables.yml"
        )
        else:
            # Default to k8s if env is neither k8s, gke or api
            airflow_variables_template_file = (
                "/init_dbt_project_files/k8s_dbt_airflow_variables.yml"
            )

        with open(airflow_variables_template_file, "r") as f:
            template_k8s = Template(f.read())

        ## Load the DBT Packages YAML template
        dbt_packages_template_file = "/init_dbt_project_files/dbt_packages.yml"
        with open(dbt_packages_template_file, "r") as f:
            template_dbt_packages = Template(f.read())

        ## Load the DBT Profiles YAML template
        dbt_profiles_template_file = "/init_dbt_project_files/dbt_profiles.yml"
        with open(dbt_profiles_template_file, "r") as f:
            template_dbt_profiles = Template(f.read())
        
        ## Load the sqlfluff configuration template
        sqlfluff_config_template_file = "/init_dbt_project_files/.sqlfluff"
        with open(sqlfluff_config_template_file, "r") as f:
            template_sqlfluff_config = Template(f.read())

        # Render the template by substituting variables with form data
        rendered_k8s_yaml = template_k8s.render(data)
        rendered_dbt_packages_yaml = template_dbt_packages.render(data)
        rendered_dbt_profiles_yaml = template_dbt_profiles.render(data)
        rendered_sqlfluff_config_yaml = template_sqlfluff_config.render(data)

        # Create a temporary directory

        temp_init_directory = "temp_init_directory_" + "".join(
            random.choices(string.ascii_uppercase + string.digits, k=8)
        )
        os.makedirs(temp_init_directory, exist_ok=True)

        # Save the rendered YAML to a temporary location
        ## Save the Airflow Variables rendered YAML
        output_file_k8s = f"{temp_init_directory}/dbt_airflow_variables.yml"
        with open(output_file_k8s, "w") as f:
            f.write(rendered_k8s_yaml)

        ## Save the DBT Packages rendered YAML
        output_file_dbt_packages = f"{temp_init_directory}/packages.yml"
        with open(output_file_dbt_packages, "w") as f:
            f.write(rendered_dbt_packages_yaml)

        ## Save the DBT Profiles rendered YAML
        output_file_dbt_profiles = f"{temp_init_directory}/profiles.yml"
        with open(output_file_dbt_profiles, "w") as f:
            f.write(rendered_dbt_profiles_yaml)
        
        ## Save the sqlfluff config rendered YAML
        output_file_sqlfluff_config = f"{temp_init_directory}/.sqlfluff"
        with open(output_file_sqlfluff_config, "w") as f:
            f.write(rendered_sqlfluff_config_yaml)

        # Cache data content for later use.
        ## Cache the API Request data
        cache_data = data

        # Create the DBT Project
        ## Required files for DBT Project Initialization
        sqlfluffignore_file = "/init_dbt_project_files/.sqlfluffignore"
        yamllint_file = "/init_dbt_project_files/yamllint-config.yaml"
        macros_set_data_tablesample = (
            "/init_dbt_project_files/set_data_tablesample.sql"
        )
        macros_generate_columns_from_airbyte_file = (
            "/init_setup_files_v1/generate_columns_from_airbyte_yml.sql"
        )

        # Get init version
        version = data["init_version"]
        airbyte_create_yml_schema_file = f"/init_setup_files_v{version}/create_yml_schema.py"
        airbyte_model_template_file = f"/init_setup_files_v{version}/airbyte_model_template.sql"

        ## Initialize the DBT Project
        os.system(
            f"echo {data['dbt_project_name']} | dbt init --skip-profile-setup --no-send-anonymous-usage-stats --quiet"
        )
        ## Copy rendered YAML files to the DBT Project
        os.system(f"mv {output_file_k8s} ./{data['dbt_project_name']}/")
        os.system(f"mv {output_file_dbt_packages} ./{data['dbt_project_name']}/")
        os.system(f"mv {output_file_dbt_profiles} ./{data['dbt_project_name']}/")
        os.system(f"mv {output_file_sqlfluff_config} ./{data['dbt_project_name']}/")
        ## Copy required files to the DBT Project
        os.system(f"cp {sqlfluffignore_file} ./{data['dbt_project_name']}/")
        os.system(f"cp {yamllint_file} ./{data['dbt_project_name']}/")

        # Update the DBT Project dbt_project.yml file with schema changes
        dbt_project_file = f"./{data['dbt_project_name']}/dbt_project.yml"
        dbt_project_name = data["dbt_project_name"]
        dbt_project_owner = data["dbt_project_owner"]

        ## Load the YAML file
        with open(dbt_project_file, "r") as file:
            data = ruamel.yaml.YAML().load(file)
        ## Check if the 'models' section exists, if not, create it
        if "models" not in data:
            data["models"] = {}

        ## Add the '+schema' configuration to each model
        for model in data["models"]:
            data["models"][model]["+schema"] = dbt_project_name
            if cache_data.get("data_quality_enabled") and cache_data.get("data_quality_enabled") == True:
                data["models"][model]["+re_data_monitored"] = True

        # Set models owner
        data["models"].setdefault("+meta", {})
        data["models"]["+meta"].update({"owner": dbt_project_owner})

        ## Add re_data model if data quality flag enabled
        if cache_data.get("data_quality_enabled") and cache_data.get("data_quality_enabled") == True:
            data["models"]["re_data"] = {
                "enable": True,
                "+schema": dbt_project_name,
                "internal": {"+schema": dbt_project_name}
        }
        ## Add the 'vars' section to the end of the file
        data["vars"] = {
            "execution_date": "{{ dbt_airflow_macros.ds(timezone=none) }}",
            "data_interval_start": "{{ data_interval_start }}",
            "data_interval_end": "{{ data_interval_end }}",
            "source_dataset_name": "source_dataset_name",
        }
        ## Save the updated YAML data back to the file
        with open(dbt_project_file, "w") as file:
            ruamel.yaml.YAML().dump(data, file)

        ## Copy the Macros files to the DBT Project if Airbyte is enabled
        if cache_data.get("airbyte_workspace_id") and cache_data.get("airbyte_workspace_id") != "None" \
                and (cache_data.get("airbyte_connection_id") and cache_data["airbyte_connection_id"] != "None"):
            ## Copy the Airbyte Model Template files to the DBT Project if Airbyte is enabled
            if version == "1":
                os.system(
                    f"cp {macros_generate_columns_from_airbyte_file} ./{cache_data['dbt_project_name']}/macros"
                )

            os.system(
                f"cp {macros_set_data_tablesample} ./{cache_data['dbt_project_name']}/macros"
            )

            os.system(
                f"cp {airbyte_model_template_file} ./{cache_data['dbt_project_name']}"
            )
            os.system(
                f"cp {airbyte_create_yml_schema_file} ./{cache_data['dbt_project_name']}"
            )
            ## Start the Airbyte DBT Project compilation process v2
            # Get the execution working directory
            execution_directory = f"./{cache_data['dbt_project_name']}"
            # Get the current working directory
            original_directory = os.getcwd()
            # Change the working directory to the execution directory
            os.chdir(execution_directory)
            # Run the Airbyte DBT Project compilation process
            # Now you are in the project directory, you can run your script
            script_path = "create_yml_schema.py"
            data_warehouse_platform = cache_data['data_warehouse_platform']
            command = f"python3 {script_path} {data_warehouse_platform}"
            exit_code = os.system(command)
            # Change back to the original working directory
            os.chdir(original_directory)
            if exit_code == 0:
                print(f"Successfully triggered {script_path} in {execution_directory}")
            else:
                print(
                    f"Error triggering {script_path} in {execution_directory}. Exit code: {exit_code}"
                )
            ## Delete the temporary script files.
            os.system(f"rm ./{cache_data['dbt_project_name']}/airbyte_model_template.sql")
            os.system(f"rm ./{cache_data['dbt_project_name']}/create_yml_schema.py")


        # Upload the DBT Project to the DBT Data Model repository (if enabled - later release)
        repo_url = os.environ.get("DATA_MODEL_REPO_URL")
        group_token = os.environ.get("GROUP_ACCESS_TOKEN")
        folder_to_copy = dbt_project_name
        temp_clone_directory = "temp_clone_directory_" + "".join(
            random.choices(string.ascii_uppercase + string.digits, k=8)
        )
        git_commit_message = f"New {folder_to_copy} DBT Project Upload"
        ## Create repo_url_final with group token
        repo_url_final = repo_url.replace(
            "https://", "https://oauth2:{}@".format(group_token)
        )
        ## Clone the GitLab repository
        repo = git.Repo.clone_from(repo_url_final, temp_clone_directory)

        reinit = cache_data.get("reinit_project", False)
        if reinit:
            source_models_path = os.path.join(os.getcwd(), folder_to_copy + '/models')
            source_macros_path = os.path.join(os.getcwd(), folder_to_copy + '/macros')
            example_folder_to_remove = os.path.join(folder_to_copy, 'models/example')
            if os.path.exists(example_folder_to_remove) and os.path.isdir(example_folder_to_remove):
                shutil.rmtree(example_folder_to_remove)

            destination_model_path = os.path.join(repo.working_dir, folder_to_copy + '/models')
            destination_macros_path = os.path.join(repo.working_dir, folder_to_copy + '/macros')
            merge_directories(source_models_path, destination_model_path)
            merge_directories(source_macros_path, destination_macros_path)
        else:
            source_path = os.path.join(os.getcwd(), folder_to_copy)
            destination_path = os.path.join(repo.working_dir, folder_to_copy)
            ## Check if the provided folder exists
            if os.path.exists(source_path):
                if os.path.isdir(source_path):
                    # Copy the folder and its contents to the destination folder
                    shutil.copytree(source_path, destination_path)
                else:
                    # If it's a file, copy the file to the destination folder
                    shutil.copy2(source_path, destination_path)


        ## Set Git user name and email
        repo.git.config("--global", "user.name", "DBT_Init_Agent")
        repo.git.config("--global", "user.email", "admin@fast.bi")
        ## Create a new branch with a random alphanumeric name (uppercase) and checkout to it
        ### Check if 'branch_name' is provided and not empty
        if "branch_name" in cache_data and cache_data["branch_name"]:
            branch_name = cache_data["branch_name"]
        else:
            # Generate a random 'branch_name' if not provided or empty
            branch_name = "DD_" + "".join(
                random.choices(string.ascii_uppercase + string.digits, k=8)
            )
        repo.git.checkout(b=branch_name)
        ## Perform Git commands
        repo.git.add("--all")
        if repo.is_dirty():
            repo.git.commit("-m", git_commit_message)
            repo.git.push(repo_url_final, branch_name)
            ## Clean up the temporary clone
            shutil.rmtree(temp_clone_directory)
            shutil.rmtree(folder_to_copy)
            shutil.rmtree(temp_init_directory)
            ## Respond Message Branch URL
            ## Create the branch URL for the response message (remove the .git extension)
            if repo_url.endswith(".git"):
                repo_url = repo_url[:-4]  # Remove the last 4 characters (".git")
            response_message_branch_url = repo_url + "/-/tree/" + branch_name

            # Create a response with download links for the compiled DBT Project

            # Respond with a JSON success message
            response = {
                "success": True,
                "New DBT Project was pushed to Branch {branch_name}. Git Repo URL": response_message_branch_url,
            }
        else:
            response = {
                "success": False,
                "error_message": 'Nothing to commit',
            }
    except Exception as e:
        # If there was an error while processing the data, respond with a JSON error message
        response = {"success": False, "error_message": str(e)}

    return response

# Define the routes
def setup_routes(app: APIBlueprint):

    # Say Hello endpoints *debug*
    @app.get("/health")
    def health():
        response = {"message": "app up and running successfully. CORS is enabled!"}
        return response

    # Kubernetes Operator (k8s) Endpoint
    @app.post('/k8s')
    @app.auth_required(auth)
    @app.doc(tags=['Project Initialization-Operators'])
    @app.input(K8SInputSchema, location='json')
    @app.output(K8SOutputSchema, status_code=201)
    def k8s_create_dbt_project(json_data):
        """Create new DBT Project with KubernetesPodOperator

        Create a list of variables for Dbt Project Initialization, after this POST command will finish, it start the deployment process.
        """

        request_data = {
            "method": request.method,
            "headers": dict(request.headers),
            "data": json_data,
        }

        env = "k8s"

        response = create_dbt_project(json_data, request_data, env)
        return jsonify(response)

    @app.post('/gke')
    @app.auth_required(auth)
    @app.doc(tags=['Project Initialization-Operators'])
    @app.input(GKEInputSchema, location='json')
    @app.output(GKEOutputSchema, status_code=201)
    def gke_create_dbt_project(json_data):
        """Create new DBT Project with GKEStartPodOperator

        Create a list of variables for Dbt Project Initialization, after this POST command will finish, it start the deployment process.

        """

        request_data = {
            "method": request.method,
            "headers": dict(request.headers),
            "data": json_data,
        }

        env = "gke"

        response = create_dbt_project(json_data, request_data, env)
        return jsonify(response)

    @app.post('/api')
    @app.auth_required(auth)
    @app.doc(tags=['Project Initialization-Operators'])
    @app.input(APIInputSchema, location='json')
    @app.output(APIOutputSchema, status_code=201)
    def api_create_dbt_project(json_data):
        """Create new DBT Project with DBTServerAPIOperator

        Create a list of variables for Dbt Project Initialization, after this POST command will finish, it start the deployment process.

        """

        request_data = {
            "method": request.method,
            "headers": dict(request.headers),
            "data": json_data,
        }

        env = "api"

        response = create_dbt_project(json_data, request_data, env)
        return jsonify(response)

    # Kubernetes Operator (k8s) Endpoint
    @app.post('/bash')
    @app.auth_required(auth)
    @app.doc(tags=['Project Initialization-Operators'])
    @app.input(BashInputSchema, location='json')
    @app.output(BashOutputSchema, status_code=201)
    def bash_create_dbt_project(json_data):
        """Create new DBT Project with BashOperator

        Create a list of variables for Dbt Project Initialization, after this POST command will finish, it start the deployment process.
        """

        request_data = {
            "method": request.method,
            "headers": dict(request.headers),
            "data": json_data,
        }

        env = "bash"

        response = create_dbt_project(json_data, request_data, env)
        return jsonify(response)
