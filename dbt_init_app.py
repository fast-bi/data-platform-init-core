# Swagger App API is Deprecated - Moved to APIFlask

import os
import json
import datetime
import random
import re
import ruamel.yaml
import git
import shutil
import string
import logging
from flask import Flask, request, jsonify, Response
from flask_swagger_ui import get_swaggerui_blueprint
from flask_cors import CORS
from functools import wraps
from flask_restful import reqparse #Not used
from jinja2 import Template

# Set up logging
logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Configure CORS for the Flask app
app = Flask(__name__)
CORS(app)
# CORS(app, origins=["http://localhost", "http://127.0.0.1"])  # Only allow requests from 'http://localhost'

# Basic authentication credentials
BASIC_AUTH_USERNAME = os.environ.get("BASIC_AUTH_USERNAME")
BASIC_AUTH_PASSWORD = os.environ.get("BASIC_AUTH_PASSWORD")

def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return username == BASIC_AUTH_USERNAME and password == BASIC_AUTH_PASSWORD

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

SWAGGER_URL = "/api/v3/ui/"
API_URL = os.environ.get("API_URL")

# Update the Swagger UI configuration to use HTTPS for all resources
swagger_ui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={"app_name": "DBT Project Initialization API", "validatorUrl": "localhost"},
)

app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)
app.logger.setLevel(logging.DEBUG)

# Create a request parser to parse the API key from the request
parser = reqparse.RequestParser()

# Retrieve the API key from the environment variable
server_api_key = os.environ.get("API_KEY")

# Define a global function to handle the common logic
def create_dbt_project(data, request_data, env):
    try:
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

        # Render the template by substituting variables with form data
        rendered_k8s_yaml = template_k8s.render(data)
        rendered_dbt_packages_yaml = template_dbt_packages.render(data)
        rendered_dbt_profiles_yaml = template_dbt_profiles.render(data)

        # Save the rendered YAML to a temporary location
        ## Save the Airflow Variables rendered YAML
        output_file_k8s = "dbt_airflow_variables.yml"
        with open(output_file_k8s, "w") as f:
            f.write(rendered_k8s_yaml)

        ## Save the DBT Packages rendered YAML
        output_file_dbt_packages = "packages.yml"
        with open(output_file_dbt_packages, "w") as f:
            f.write(rendered_dbt_packages_yaml)

        ## Save the DBT Profiles rendered YAML
        output_file_dbt_profiles = "profiles.yml"
        with open(output_file_dbt_profiles, "w") as f:
            f.write(rendered_dbt_profiles_yaml)

        # Cache data content for later use.
        ## Cache the API Request data
        cache_data = data

        # Create the DBT Project
        ## Required files for DBT Project Initialization
        sqlfluff_file = "/init_dbt_project_files/.sqlfluff"
        sqlfluffignore_file = "/init_dbt_project_files/.sqlfluffignore"
        yamllint_file = "/init_dbt_project_files/yamllint-config.yaml"
        macros_generate_columns_from_airbyte_file = (
            "/init_setup_files/generate_columns_from_airbyte_yml.sql"
        )
        airbyte_model_template_file = "/init_setup_files/airbyte_model_template.sql"
        airbyte_create_yml_schema_file = "/init_setup_files/create_yml_schema.py"
        ## Initialize the DBT Project
        os.system(
            f"echo {data['dbt_project_name']} | dbt init --skip-profile-setup --no-send-anonymous-usage-stats --quiet"
        )
        ## Copy rendered YAML files to the DBT Project
        os.system(f"mv {output_file_k8s} ./{data['dbt_project_name']}/")
        os.system(f"mv {output_file_dbt_packages} ./{data['dbt_project_name']}/")
        os.system(f"mv {output_file_dbt_profiles} ./{data['dbt_project_name']}/")
        ## Copy required files to the DBT Project
        os.system(f"cp {sqlfluff_file} ./{data['dbt_project_name']}/")
        os.system(f"cp {sqlfluffignore_file} ./{data['dbt_project_name']}/")
        os.system(f"cp {yamllint_file} ./{data['dbt_project_name']}/")

        # Update the DBT Project dbt_project.yml file with schema changes
        dbt_project_file = f"./{data['dbt_project_name']}/dbt_project.yml"
        dbt_project_name = data["dbt_project_name"]

        ## Load the YAML file
        with open(dbt_project_file, "r") as file:
            data = ruamel.yaml.YAML().load(file)
        ## Check if the 'models' section exists, if not, create it
        if "models" not in data:
            data["models"] = {}
        ## Add the '+schema' configuration to each model
        for model in data["models"]:
            data["models"][model]["+schema"] = dbt_project_name
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
            os.system(
                f"cp {macros_generate_columns_from_airbyte_file} ./{cache_data['dbt_project_name']}/macros"
            )
            os.system(
                f"cp {airbyte_model_template_file} ./{cache_data['dbt_project_name']}"
            )
            os.system(
                f"cp {airbyte_create_yml_schema_file} ./{cache_data['dbt_project_name']}"
            )
            # ## Start the Airbyte DBT Project compilation process
            # script_path = f"./{data['dbt_project_name']}/create_yml_schema.py"
            # result = subprocess.run(["python3", script_path], capture_output=True, text=True)
            # ## Print the output of the Airbyte DBT Project compilation process
            # if result.returncode == 0:
            #     print("Script executed successfully")
            #     print(result.stdout)
            # else:
            #     print("Script execution failed")
            #     print(result.stderr)
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
            command = f"python3 {script_path}"
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
        ## Copy the provided folder from the script's current directory
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
        repo.git.commit("-m", git_commit_message)
        repo.git.push(repo_url_final, branch_name)
        ## Clean up the temporary clone
        shutil.rmtree(temp_clone_directory)
        shutil.rmtree(folder_to_copy)
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
    except Exception as e:
        # If there was an error while processing the data, respond with a JSON error message
        response = {"success": False, "error_message": str(e)}

    return response

# API Key Validation
excluded_paths = ["/", "/api/v3/health", "/favicon.ico"]
included_paths = ["/api/v3/k8s", "/api/v3/gke", "/api/v3/api"]

@app.before_request
def before_request():
    auth = request.authorization
    if request.path.startswith("/api/v3/ui"):
        # Basic Authentication for UI
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
    elif request.path.startswith(tuple(included_paths)):
        # API Key Validation for included paths
        api_key = request.args.get("X-API-KEY")
        if not api_key or api_key != server_api_key:
            return jsonify({"error": "Invalid or missing API key"}), 401

    # Proceed with the request if no authentication errors were encountered
    return None

@app.route("/api/v3/health")
def home():
    response = jsonify({"message": "app up and running successfully. CORS is enabled!"})
    print(response.headers)  # Check if CORS headers are present
    return response

@app.route("/api/v3/k8s", methods=["POST"])
def k8s_create_dbt_project():
    """Create new DBT Project with KubernetesPodOperator

    Create a list of variables for Dbt Project Initialization, after this POST command will finish, it start the deployment process. # noqa: E501

    :param workload_platform:
    :type workload_platform: str
    :param platform_namespace:
    :type platform_namespace: str
    :param operator:
    :type operator: str
    :param dbt_k8s_pod_name:
    :type dbt_k8s_pod_name: str
    :param data_model_repository_url:
    :type data_model_repository_url: str
    :param dbt_project_name:
    :type dbt_project_name: str
    :param dbt_dag_schedule_interval:
    :type dbt_dag_schedule_interval: str
    :param dbt_dag_tag:
    :type dbt_dag_tag: str
    :param gcp_project_id:
    :type gcp_project_id: str
    :param project_level:
    :type project_level: str
    :param tsb_dbt_core_image_version:
    :type tsb_dbt_core_image_version: str
    :param dbt_seed_enabled:
    :type dbt_seed_enabled: bool
    :param dbt_seed_sharding_enabled:
    :type dbt_seed_sharding_enabled: bool
    :param dbt_seed_check_period:
    :type dbt_seed_check_period: str
    :param dbt_snapshot_enabled:
    :type dbt_snapshot_enabled: bool
    :param dbt_snapshot_sharding_enabled:
    :type dbt_snapshot_sharding_enabled: bool
    :param dbt_debug:
    :type dbt_debug: bool
    :param dbt_model_debug_log:
    :type dbt_model_debug_log: bool
    :param data_quality_enabled:
    :type data_quality_enabled: bool
    :param datahub_enabled:
    :type datahub_enabled: bool
    :param airbyte_workspace_id:
    :type airbyte_workspace_id: str
    :param airbyte_connection_id:
    :type airbyte_connection_id: str
    :param airbyte_connection_name:
    :type airbyte_connection_name: str
    :param branch_name:
    :type branch_name: str

    :rtype: K8SDBTProjectVariables
    """

    request_data = {
        "method": request.method,
        "headers": dict(request.headers),
        "data": request.data.decode("utf-8"),
        "form": dict(request.form),
        "files": [(key, file.filename) for key, file in request.files.items()],
    }

    content_type = request.headers.get("Content-Type")

    if content_type == "application/json":
        data = request.get_json()
    elif content_type == "application/x-www-form-urlencoded":
        data = request.form
    elif re.search(r"form-data", content_type):
        data = request.form
    else:
        return jsonify({"error": "Unsupported Content-Type"}), 400

    env = "k8s"

    response = create_dbt_project(data, request_data, env)
    return jsonify(response)


@app.route("/api/v3/gke", methods=["POST"])
def gke_create_dbt_project():
    """Create new DBT Project with GKEStartPodOperator

    Create a list of variables for Dbt Project Initialization, after this POST command will finish, it start the deployment process. # noqa: E501

    :param cluster_name:
    :type cluster_name: str
    :param cluster_zone:
    :type cluster_zone: str
    :param cluster_node_count:
    :type cluster_node_count: str
    :param cluster_machine_type:
    :type cluster_machine_type: str
    :param cluster_machine_disk_type:
    :type cluster_machine_disk_type: str
    :param network:
    :type network: str
    :param subnetwork:
    :type subnetwork: str
    :param privatenodes_ip_range:
    :type privatenodes_ip_range: str
    :param shared_vpc:
    :type shared_vpc: bool
    :param services_secondary_range_name:
    :type services_secondary_range_name: str
    :param cluster_secondary_range_name:
    :type cluster_secondary_range_name: str
    :param workload_platform:
    :type workload_platform: str
    :param platform_namespace:
    :type platform_namespace: str
    :param operator:
    :type operator: str
    :param dbt_k8s_pod_name:
    :type dbt_k8s_pod_name: str
    :param data_model_repository_url:
    :type data_model_repository_url: str
    :param dbt_project_name:
    :type dbt_project_name: str
    :param dbt_dag_schedule_interval:
    :type dbt_dag_schedule_interval: str
    :param dbt_dag_tag:
    :type dbt_dag_tag: str
    :param gcp_project_id:
    :type gcp_project_id: str
    :param project_level:
    :type project_level: str
    :param tsb_dbt_core_image_version:
    :type tsb_dbt_core_image_version: str
    :param dbt_seed_enabled:
    :type dbt_seed_enabled: bool
    :param dbt_seed_sharding_enabled:
    :type dbt_seed_sharding_enabled: bool
    :param dbt_seed_check_period:
    :type dbt_seed_check_period: str
    :param dbt_snapshot_enabled:
    :type dbt_snapshot_enabled: bool
    :param dbt_snapshot_sharding_enabled:
    :type dbt_snapshot_sharding_enabled: bool
    :param dbt_debug:
    :type dbt_debug: bool
    :param dbt_model_debug_log:
    :type dbt_model_debug_log: bool
    :param data_quality_enabled:
    :type data_quality_enabled: bool
    :param datahub_enabled:
    :type datahub_enabled: bool
    :param airbyte_workspace_id:
    :type airbyte_workspace_id: str
    :param airbyte_connection_id:
    :type airbyte_connection_id: str
    :param airbyte_connection_name:
    :type airbyte_connection_name: str
    :param branch_name:
    :type branch_name: str

    :rtype: GKEDBTProjectVariables
    """

    request_data = {
        "method": request.method,
        "headers": dict(request.headers),
        "data": request.data.decode("utf-8"),
        "form": dict(request.form),
        "files": [(key, file.filename) for key, file in request.files.items()],
    }

    content_type = request.headers.get("Content-Type")

    if content_type == "application/json":
        data = request.get_json()
    elif content_type == "application/x-www-form-urlencoded":
        data = request.form
    elif re.search(r"form-data", content_type):
        data = request.form
    else:
        return jsonify({"error": "Unsupported Content-Type"}), 400

    env = "gke"

    response = create_dbt_project(data, request_data, env)
    return jsonify(response)

@app.route("/api/v3/api", methods=["POST"])
def api_create_dbt_project():
    """Create new DBT Project with ApiOperator

    Create a list of variables for Dbt Project Initialization, after this POST command will finish, it start the deployment process. # noqa: E501

    :param workload_platform:
    :type workload_platform: str
    :param platform_namespace:
    :type platform_namespace: str
    :param operator:
    :type operator: str
    :param data_model_repository_url:
    :type data_model_repository_url: str
    :param dbt_project_name:
    :type dbt_project_name: str
    :param dbt_dag_schedule_interval:
    :type dbt_dag_schedule_interval: str
    :param dbt_dag_tag:
    :type dbt_dag_tag: str
    :param gcp_project_id:
    :type gcp_project_id: str
    :param project_level:
    :type project_level: str
    :param tsb_dbt_core_image_version:
    :type tsb_dbt_core_image_version: str
    :param dbt_seed_enabled:
    :type dbt_seed_enabled: bool
    :param dbt_seed_sharding_enabled:
    :type dbt_seed_sharding_enabled: bool
    :param dbt_seed_check_period:
    :type dbt_seed_check_period: str
    :param dbt_snapshot_enabled:
    :type dbt_snapshot_enabled: bool
    :param dbt_snapshot_sharding_enabled:
    :type dbt_snapshot_sharding_enabled: bool
    :param dbt_debug:
    :type dbt_debug: bool
    :param dbt_model_debug_log:
    :type dbt_model_debug_log: bool
    :param data_quality_enabled:
    :type data_quality_enabled: bool
    :param datahub_enabled:
    :type datahub_enabled: bool
    :param airbyte_workspace_id:
    :type airbyte_workspace_id: str
    :param airbyte_connection_id:
    :type airbyte_connection_id: str
    :param airbyte_connection_name:
    :type airbyte_connection_name: str
    :param branch_name:
    :type branch_name: str

    :rtype: APIDBTProjectVariables
    """

    request_data = {
        "method": request.method,
        "headers": dict(request.headers),
        "data": request.data.decode("utf-8"),
        "form": dict(request.form),
        "files": [(key, file.filename) for key, file in request.files.items()],
    }

    content_type = request.headers.get("Content-Type")

    if content_type == "application/json":
        data = request.get_json()
    elif content_type == "application/x-www-form-urlencoded":
        data = request.form
    elif re.search(r"form-data", content_type):
        data = request.form
    else:
        return jsonify({"error": "Unsupported Content-Type"}), 400

    env = "api"

    response = create_dbt_project(data, request_data, env)
    return jsonify(response)

if __name__ == "__main__":
    # Determine the environment and configure Flask accordingly
    if os.environ.get("FLASK_ENV") == "production":
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8888)))
    else:
        app.run(host="0.0.0.0", port=8888, debug=True)
