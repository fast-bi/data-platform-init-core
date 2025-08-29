import re

import requests
import yaml
import os
import sys
from collections import defaultdict

global DATA_WAREHOUSE_PLATFORM
DATA_WAREHOUSE_PLATFORM = sys.argv[1]

def read_airflow_var_yml(file_path):
    with open(file_path, "r") as file:
        airflow_var = yaml.safe_load(file)
    return airflow_var


def read_api_connection_list(url, workspace_id):
    headers = {"Content-Type": "application/json"}
    json_obj = {"workspaceId": workspace_id}
    x = requests.post(url, json=json_obj, headers=headers)
    return x.json()


def create_yml_file(yml_dict, file_path, file_name):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    with open(f"{file_path}/{file_name}.yml", "w") as yaml_file:
        yaml.dump(yml_dict, yaml_file, default_flow_style=False, sort_keys=False)


def convert_value_to_system_standard(val: str) -> str:
    """
        Converts a string to a system-standard format by applying several transformations:
        - Adds underscores between camelCase or PascalCase words.
        - Replaces spaces and dots with underscores.
        - Replaces multiple underscores with a single underscore.
        - Removes leading underscores.
        - Converts the entire string to lowercase.

        Parameters:
            val (str): The input string to be transformed.

        Returns:
            str: The transformed string in system-standard format.
    """

    if not isinstance(val, str):
        raise ValueError("Input must be a string")

    new_val = re.sub(
        "(.)([A-Z][a-z]+)", r"\1_\2", val
    )  # adding underscore between first and second found groups
    new_val = re.sub(r"\s", "_", new_val)  # replace all spaces
    new_val = re.sub(r"_+", "_", new_val)  # replace n-underscores to 1 inside the name
    new_val = re.sub(r"\b_+", "", new_val)  # remove leading underscores
    new_val = re.sub(r"\.", "_", new_val)  # replace dots to underscores
    new_val = re.sub(
        "([a-z0-9])([A-Z])", r"\1_\2", new_val
    )  # adding underscore before upper case letter if starts from lower case or digit
    return new_val.lower()


def quote_value_with_dot(val: str) -> str:
    """
    Wraps each part of a dot-separated string in backticks.

    Parameters:
    val (str): The input string to be processed.

    Returns:
    str: A string where each part is wrapped in backticks and separated by dots.
    """
    if not isinstance(val, str):
        raise ValueError("Input must be a string")
    r = None
    if DATA_WAREHOUSE_PLATFORM == 'snowflake':
        r = ':'.join(f"{part}" for part in val.split('.'))
    elif DATA_WAREHOUSE_PLATFORM == 'redshift':
        if '.' in val:
            json_column, json_key = val.split('.',1)
            r = f"json_extract_path_text(json_serialize({json_column}), '{json_key}')"
        else:
            r = val
    else:
        r = '.'.join(f"`{part}`" for part in val.split('.'))

    return r


def type_convert(col, col_type):
    if isinstance(col_type, list):
        for item in col_type:
            if item:
                source_col_type = item
    else:
        source_col_type = col_type

    type_dict = {
        "number": "numeric",
        "timestamp_without_timezone": "timestamp",
        "timestamp_with_timezone": "timestamp",
        "object": "string",
        "boolean": "bool",
        "array": "array",
        "integer": "integer",
    }

    if "date" in col.lower():
        type_dict["string"] = "timestamp"
    return (
        type_dict.get(source_col_type)
        if source_col_type in type_dict.keys()
        else "string"
    ) or source_col_type


def delete_null_from_list(data):
    if isinstance(data, list):
        data_without_null = [x for x in data if x != "null"]
        if len(data_without_null) == 1:
            return data_without_null[0]
        else:
            return "string"
    else:
        return data


def create_model(source_name: str, source_table_name: str, t_name: str, columns: list[str]) -> None:
    """
    Generates a SQL model file by replacing placeholders in a template with the provided source name, table name, and columns.

    Parameters:
    source_name (str): The name of the data source.
    table_name (str): The name of the source table.
    columns (list[str]): A list of column names to be transformed and included in the SQL model.

    Returns:
    None
    """
    if not isinstance(source_name, str) or not isinstance(t_name, str) or not isinstance(columns, list):
        raise ValueError("Invalid input types. Expected str for source_name and table_name, and list[str] for columns.")

    template_path = "airbyte_model_template.sql"

    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file '{template_path}' not found.")

    with open(template_path, mode="r", encoding="utf-8") as template_file:
        file_data = template_file.read()

    if DATA_WAREHOUSE_PLATFORM == 'snowflake':
        formatted_columns = ',\n               '.join(
            f"{quote_value_with_dot(c)} as {convert_value_to_system_standard(c)}" for c in columns)
    elif DATA_WAREHOUSE_PLATFORM == 'redshift':
        formatted_columns = ',\n               '.join(
            f"{quote_value_with_dot(c)} as {convert_value_to_system_standard(c)}" for c in columns)
    else:
        formatted_columns = ',\n               '.join(
            f"{quote_value_with_dot(c)} as `{convert_value_to_system_standard(c)}`" for c in columns)

    file_data = file_data.replace("source_name", source_name)
    file_data = file_data.replace("source_table_name", source_table_name)
    file_data = file_data.replace("fields", formatted_columns)

    output_path = f"models/staging/{source_name}/{t_name}.sql"

    with open(output_path, mode="w", encoding="utf-8") as output_file:
        output_file.write(file_data)


def add_source_column(col, col_name, col_type, col_desc=""):
    col_type = delete_null_from_list(col_type)
    col.append(
        {
            "name": col_name,
            "data_type": type_convert(col_name, col_type),
            "description": col_desc,
        }
    )


def add_model_column(col, col_name, col_type, col_desc=""):
    col_type = delete_null_from_list(col_type)
    col.append(
        {
            "name": convert_value_to_system_standard(col_name),
            "identifier": col_name,
            "data_type": type_convert(col_name, col_type),
            "description": col_desc,
        }
    )


def create_source_yml_dict(tb, tb_name, prefix_with_tb_name):
    source_columns = []
    model_columns = []
    for col in tb["stream"]["jsonSchema"]["properties"]:
        if tb["stream"]["jsonSchema"]["properties"][col].get("airbyte_type"):
            col_type = tb["stream"]["jsonSchema"]["properties"][
                col
            ].get("airbyte_type")
            col_desc = tb["stream"]["jsonSchema"]["properties"][
                col
            ].get("description", "")
            add_source_column(source_columns, col, col_type, col_desc)
            add_model_column(model_columns, col, col_type, col_desc)

        elif tb["stream"]["jsonSchema"]["properties"][col].get("anyOf"):
            col_items = [
                d["properties"]
                for i, d in enumerate(
                    tb["stream"]["jsonSchema"]["properties"][col].get("anyOf")
                )
                if "properties" in d.keys()
            ]
            if col_items:
                for sub_col_obj_prop in col_items:
                    for item in sub_col_obj_prop:
                        sub_col_name = col + "." + item
                        sub_col_type = sub_col_obj_prop[item].get("type", "string")
                        sub_col_type = delete_null_from_list(sub_col_type)
                        sub_col_desc = sub_col_obj_prop[item].get("description", "")
                        add_source_column(source_columns, sub_col_name, sub_col_type, sub_col_desc)
                        add_model_column(model_columns, sub_col_name, sub_col_type, sub_col_desc)
            else:
                add_source_column(source_columns, col, "array", "")
                add_model_column(model_columns, col, "array", "")

        else:
            col_type = tb["stream"]["jsonSchema"]["properties"][col].get("type")

            if isinstance(col_type, list):
                col_type = delete_null_from_list(col_type)

            if col_type == "object":
                if "properties" in tb["stream"]["jsonSchema"]["properties"][col]:
                    for col_obj_prop in tb["stream"]["jsonSchema"]["properties"][col]["properties"]:
                        sub_col_name = col + "." + col_obj_prop
                        sub_col_type = tb["stream"]["jsonSchema"]["properties"][col]["properties"][col_obj_prop]["type"]
                        sub_col_type = delete_null_from_list(sub_col_type)
                        sub_col_desc = tb["stream"]["jsonSchema"]["properties"][col]["properties"][col_obj_prop].get(
                            "description", "")

                        if sub_col_type != "object":
                            add_source_column(source_columns, sub_col_name, sub_col_type, sub_col_desc)
                            add_model_column(model_columns, sub_col_name, sub_col_type, sub_col_desc)
                        else:
                            col_info = tb["stream"]["jsonSchema"]["properties"][col]["properties"][col_obj_prop]
                            for sub_col_obj_prop in col_info["properties"]:
                                sub_sub_col_name = (sub_col_name + "." + sub_col_obj_prop)
                                sub_sub_col_type = col_info["properties"][sub_col_obj_prop].get("type", "string")
                                sub_sub_col_desc = col_info["properties"][sub_col_obj_prop].get("description", "")
                                add_source_column(source_columns, sub_sub_col_name, sub_sub_col_type,
                                                  sub_sub_col_desc, )
                                add_model_column(model_columns, sub_sub_col_name, sub_sub_col_type, sub_sub_col_desc, )
            else:
                add_source_column(source_columns, col, col_type,
                                  tb["stream"]["jsonSchema"]["properties"][col].get("description", ""))
                add_model_column(model_columns, col, col_type,
                                 tb["stream"]["jsonSchema"]["properties"][col].get("description", ""))

    constraints = [
        item
        for sublist in tb["config"].get("primaryKey")
        for item in sublist
        if sublist != "null"
    ]

    for i in model_columns:
        if i["name"] in constraints:
            i["data_tests"] = [
                "not_null",
                "unique"
            ]
    source_yml_dict = {"name": prefix_with_tb_name,
                       "identifier": prefix_with_tb_name,
                       "description": "",
                       "columns": source_columns,
                       "freshness": {
                           "warn_after": {"count": 24, "period": "hour"},
                           "error_after": {"count": 36, "period": "hour"}
                       },
                       "loaded_at_field": "_airbyte_extracted_at"
                       }
    model_yml_dict = {"name": tb_name,
                      "description": "",
                      "columns": model_columns
                      }
    return source_yml_dict, model_yml_dict


def group_by_namespace(data):
    grouped = defaultdict(list)

    for item in data.get("syncCatalog", {}).get("streams", []):
        namespace = item.get("stream", {}).get("namespace", "unknown")
        grouped[namespace].append(item)

    new_sync_catalog = {"syncCatalog": []}
    for namespace, streams in grouped.items():
        new_sync_catalog["syncCatalog"].append({
            "namespace": namespace,
            "streams": streams
        })

    return new_sync_catalog


# ------------------- init part -----------------------------
airflow_variables_list = os.environ.get("AIRFLOW_VARIABLES_FILE_NAME")
airflow_var = read_airflow_var_yml(airflow_variables_list)
connection_ids = list(airflow_var.values())[0].get("AIRBYTE_CONNECTION_ID")

if connection_ids and connection_ids != "None":
    workspace_id = list(airflow_var.values())[0].get("AIRBYTE_WORKSPACE_ID")

    # read data from API
    airbyte_url = os.environ.get("AIRBYTE_LOCAL_K8S_SVC_URL")

    url_connections = f"http://{airbyte_url}/api/v1/connections/list"
    url_destination = f"http://{airbyte_url}/api/v1/destinations/list"

    api_request_json = read_api_connection_list(url_connections, workspace_id)
    destination_info = read_api_connection_list(url_destination, workspace_id)

    # create yml schema files
    source_array = []
    dataset = ""
    database = ""
    for connection_id in connection_ids:
        for dest in destination_info["destinations"]:
            if dest["workspaceId"] == workspace_id:

                destination_id = dest.get("destinationId")

                for i in api_request_json["connections"]:
                    if i.get("destinationId") == destination_id and i.get("connectionId") == connection_id:
                        if DATA_WAREHOUSE_PLATFORM in ['bigquery', '', None]:
                            database = dest["connectionConfiguration"]["project_id"]
                        else:
                            database = dest["connectionConfiguration"]["database"]
                        g = group_by_namespace(i)

                        for s in g["syncCatalog"]:
                            sources = []
                            for k in s["streams"]:
                                if s.get('namespace'):
                                    dataset = i["namespaceFormat"].replace("${SOURCE_NAMESPACE}", s['namespace'])
                                elif "namespaceFormat" in i:
                                    dataset = i["namespaceFormat"]
                                else:
                                    dataset = dest["connectionConfiguration"]["dataset_id"]

                                prefix_with_name = i.get("prefix") + k["stream"]["name"]
                                table_name = 'stg_' + prefix_with_name
                                col_list = []
                                source_dict, model_dict = create_source_yml_dict(k, table_name, prefix_with_name)
                                sources.append(source_dict)

                                for col in model_dict['columns']:
                                    col_name = col.pop('identifier', None)
                                    col_list.append(col_name)

                                model = {
                                    "version": 2,
                                    "models": [model_dict]}
                                create_yml_file(model, f"models/staging/{dataset}", table_name)
                                create_model(dataset, prefix_with_name, table_name, col_list)
                            source = {"name": dataset,
                                      "database": database,
                                      'tables': sources}
                            source_array.append(source)

    result = {
        "version": 2,
        "sources": source_array}
    create_yml_file(result, "models", "source")
