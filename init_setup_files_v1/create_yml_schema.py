import requests
import yaml
import re
import os
import ruamel.yaml


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


def update_dbt_project_file(my_dataset_variable):
    # Define the filename of the dbt project file
    dbt_project_file = "dbt_project.yml"
    # Load the YAML file
    yaml = ruamel.yaml.YAML()
    with open(dbt_project_file, "r") as file:
        dbt_project_data = yaml.load(file)
    # Check if the 'vars' section exists and 'source_dataset_name' is defined
    if "vars" in dbt_project_data and "source_dataset_name" in dbt_project_data["vars"]:
        # Update the 'source_dataset_name' variable
        dbt_project_data["vars"]["source_dataset_name"] = my_dataset_variable
    # Write the updated YAML data back to the file
    with open(dbt_project_file, "w") as file:
        yaml.dump(dbt_project_data, file)


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

    return (
        type_dict.get(source_col_type)
        if source_col_type in type_dict.keys()
        else "string"
    ) or source_col_type


def convert_value_to_system_standart(val):
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


def create_source_yml(parsed_json, connection_id, database, dataset, destination_id):
    yml_dict = {"version": 2}
    tbl = {}
    for con_id in parsed_json["connections"]:
        if (
            con_id.get("connectionId") == connection_id
            and con_id.get("destinationId") == destination_id
        ):

            if "namespaceFormat" in con_id and not ("$" in con_id["namespaceFormat"]):
                new_dataset = con_id["namespaceFormat"]
            else:
                new_dataset = dataset

            prefix = con_id.get("prefix", '')

            update_dbt_project_file(new_dataset)
            yml_dict.setdefault("sources", []).append(
                {"name": new_dataset, "database": database}
            )
            for tb in con_id["syncCatalog"]["streams"]:
                tbl.setdefault("tables", []).append(
                    {
                        "name": "raw_"
                        + convert_value_to_system_standart(tb["stream"]["name"]),
                        "identifier": "_airbyte_raw_" + prefix + tb["stream"]["name"],
                        "description": "",
                    }
                )
                #                 for col in tb['stream']['jsonSchema']['properties']:
                #                     if tb['stream']['jsonSchema']['properties'][col].get('airbyte_type'):
                #                         col_type = tb['stream']['jsonSchema']['properties'][col].get('airbyte_type')
                #                     else:
                #                         col_type = tb['stream']['jsonSchema']['properties'][col].get('type')
                for i in tbl["tables"]:
                    i.setdefault("columns", []).append(
                        {
                            "name": "_airbyte_ab_id",
                            "description": "Transaction ID",
                            "data_type": "string",
                        }
                    )
                    i["columns"].append(
                        {
                            "name": "_airbyte_emitted_at",
                            "description": "Transaction Import Time",
                            "data_type": "timestamp",
                        }
                    )
                    i["columns"].append(
                        {
                            "name": "_airbyte_data",
                            "description": "Transaction Data",
                            "data_type": "string",
                        }
                    )
                    i["freshness"] = {
                        "warn_after": {"count": 24, "period": "hour"},
                        "error_after": {"count": 36, "period": "hour"}}
                    i["loaded_at_field"] = "_airbyte_emitted_at"

                for item in yml_dict["sources"]:
                    item.setdefault("tables", []).append(*tbl["tables"])
                tbl = {}
    return yml_dict


def add_column(col, parent_col, col_name, col_type, col_desc=""):
    col_type = delete_null_from_list(col_type)
    col.setdefault(parent_col, []).append(
        {
            "name": convert_value_to_system_standart(col_name),
            "identifier": col_name,
            "data_type": type_convert(col_name, col_type),
            "description": col_desc,
        }
    )


def delete_null_from_list(data):
    if isinstance(data, list):
        data_without_null = [x for x in data if x != "null"]
        if len(data_without_null) == 1:
            return data_without_null[0]
        else:
            return "string"
    else:
        return data


def create_yml_dict(parsed_json, connection_id, table_name):
    yml_dict = {"version": 2}
    for con_id in parsed_json["connections"]:
        if con_id.get("connectionId") == connection_id:
            for tb in con_id["syncCatalog"]["streams"]:
                if tb["stream"]["name"] == table_name:
                    yml_dict.setdefault("models", []).append(
                        {
                            "name": "stg_"
                                    + convert_value_to_system_standart(tb["stream"]["name"])
                        }
                    )

                    constraints = [
                        item
                        for sublist in tb["config"].get("primaryKey")
                        for item in sublist
                        if sublist != "null"
                    ]
                    if tb["stream"]["jsonSchema"].get("properties"):
                        for col in tb["stream"]["jsonSchema"]["properties"]:
                            for te in yml_dict["models"]:
                                if tb["stream"]["jsonSchema"]["properties"][col].get("airbyte_type"):
                                    col_type = tb["stream"]["jsonSchema"]["properties"][col].get("airbyte_type")
                                    col_desc = tb["stream"]["jsonSchema"]["properties"][col].get("description", "")
                                    add_column(te, "columns", col, col_type, col_desc)
                                elif tb["stream"]["jsonSchema"]["properties"][col].get("anyOf"):
                                    col_items = [d["properties"] for i, d in enumerate(
                                        tb["stream"]["jsonSchema"]["properties"][col].get("anyOf"))
                                                 if "properties" in d.keys()]
                                    if col_items:
                                        for sub_col_obj_prop in col_items:
                                            for item in sub_col_obj_prop:
                                                sub_col_name = col + "." + item
                                                sub_col_type = sub_col_obj_prop[item].get("type", "string")
                                                sub_col_type = delete_null_from_list(sub_col_type)
                                                sub_col_desc = sub_col_obj_prop[item].get("description", "")
                                                add_column(
                                                    te,
                                                    "columns",
                                                    sub_col_name,
                                                    sub_col_type,
                                                    sub_col_desc, )
                                    else:
                                        add_column(te, "columns", col, "array", "")

                                else:
                                    col_type = tb["stream"]["jsonSchema"]["properties"][col].get("type")
                                    if isinstance(col_type, list):
                                        col_type = delete_null_from_list(col_type)
                                    col_desc = tb["stream"]["jsonSchema"]["properties"][col].get("description")
                                    if col_type == "object":
                                        if tb["stream"]["jsonSchema"]["properties"][col].get("properties"):
                                            for col_obj_prop in tb["stream"]["jsonSchema"]["properties"][col][
                                                "properties"]:
                                                sub_col_name = col + "." + col_obj_prop
                                                sub_col_type = \
                                                tb["stream"]["jsonSchema"]["properties"][col]["properties"][
                                                    col_obj_prop]["type"]
                                                sub_col_type = delete_null_from_list(sub_col_type)
                                                sub_col_desc = \
                                                tb["stream"]["jsonSchema"]["properties"][col]["properties"][
                                                    col_obj_prop].get(
                                                    "description", "")

                                                if sub_col_type != "object":
                                                    add_column(
                                                        te,
                                                        "columns",
                                                        sub_col_name,
                                                        sub_col_type,
                                                        sub_col_desc, )
                                                else:
                                                    col_info = \
                                                    tb["stream"]["jsonSchema"]["properties"][col]["properties"][
                                                        col_obj_prop]
                                                    if col_info.get("properties"):
                                                        for sub_col_obj_prop in col_info["properties"]:
                                                            # print(col_info["properties"][sub_col_obj_prop])
                                                            col_sub_sub = col_info["properties"][sub_col_obj_prop].get(
                                                                "properties")
                                                            if col_sub_sub:
                                                                for i in col_sub_sub:
                                                                    col_sub_sub_name = sub_col_name + "." + sub_col_obj_prop + "." + i
                                                                    print(col_sub_sub_name)
                                                                    col_sub_sub_type_ = col_sub_sub[i].get("type",
                                                                                                           "string")
                                                                    col_sub_sub_type = delete_null_from_list(
                                                                        col_sub_sub_type_)
                                                                    col_sub_sub_desc = col_sub_sub[i].get("description",
                                                                                                          "")
                                                                    add_column(
                                                                        te,
                                                                        "columns",
                                                                        col_sub_sub_name,
                                                                        col_sub_sub_type,
                                                                        col_sub_sub_desc, )

                                                            sub_sub_col_name = (sub_col_name + "." + sub_col_obj_prop)

                                                            sub_sub_col_type = col_info["properties"][
                                                                sub_col_obj_prop].get(
                                                                "type", "string")

                                                            sub_sub_col_desc = col_info["properties"][
                                                                sub_col_obj_prop].get(
                                                                "description", "")
                                                            add_column(
                                                                te,
                                                                "columns",
                                                                sub_sub_col_name,
                                                                sub_sub_col_type,
                                                                sub_sub_col_desc, )
                                        else:
                                            add_column(te,
                                                       "columns",
                                                       col,
                                                       col_type,
                                                       col_desc, )
                                    else:
                                        add_column(
                                            te,
                                            "columns",
                                            col,
                                            col_type,
                                            tb["stream"]["jsonSchema"]["properties"][
                                                col
                                            ].get("description", ""),
                                        )

                                for i in te["columns"]:
                                    if i["identifier"] in constraints:
                                        i["constraints"] = [
                                            {"type": "not_null"},
                                            {"type": "unique"},
                                            {"type": "primary_key"},
                                        ]
    for i in yml_dict["models"]:
        i["columns"].append(
            {
                "name": "execution_date",
                "identifier": "execution_date",
                "data_type": "date",
                "description": str(
                    "The date when the replicated data was normalized into the data destination system."),
            }
        )
        i["columns"].append(
            {
                "name": "ab_id",
                "identifier": "_airbyte_ab_id",
                "data_type": "string",
                "description": str("A unique ID for the data replication record into the destination system."),
            }
        )
        i["columns"].append(
            {
                "name": "ab_emitted_at",
                "identifier": "_airbyte_emitted_at",
                "data_type": "string",
                "description": str("The date when the replicated data was loaded into the data destination system."),
            }
        )
        i["columns"].append(
            {
                "name": "unique_id",
                "identifier": "unique_id",
                "data_type": "string",
                "description": str("A Unique_id is a surrogate key from the source system's primary key or keys."),
                "data_tests": ["not_null", "unique"],
            }
        )
    return yml_dict, [convert_value_to_system_standart(i) for i in constraints]


def create_model(table_name, new_table_name, col_list=None, date_col=None, unique_key_list=None):
    with open("airbyte_model_template.sql", mode="r", encoding="utf-8") as file:
        filedata = file.read()
        if not unique_key_list and col_list:
            unique_key_list = col_list

        # Replace the target string execution_date to date_col
        if date_col:
            filedata = filedata.replace("execution_date", date_col.lower())
            filedata = filedata.replace("date_col", date_col)
        else:
            filedata = filedata.replace("date_col", "execution_date")

        filedata = filedata.replace("source_table_name", f"raw_{new_table_name}")
        filedata = filedata.replace("table_name", f"stg_{new_table_name}")
        filedata = filedata.replace("unique_key_list", ", ".join(unique_key_list))

    with open(f"models/staging/stg_{new_table_name}.sql", "w") as file:
        file.write(filedata)


# ------------------- init part -----------------------------

airflow_variables_list = os.environ.get("AIRFLOW_VARIABLES_FILE_NAME")
airflow_var = read_airflow_var_yml(airflow_variables_list)
connection_id = list(airflow_var.values())[0].get("AIRBYTE_CONNECTION_ID")

if connection_id and connection_id != "None":
    workspace_id = list(airflow_var.values())[0].get("AIRBYTE_WORKSPACE_ID")

    # read data from API
    airbyte_url = os.environ.get("AIRBYTE_LOCAL_K8S_SVC_URL")

    url_connections = f"http://{airbyte_url}/api/v1/connections/list"
    url_destination = f"http://{airbyte_url}/api/v1/destinations/list"

    api_request_json = read_api_connection_list(url_connections, workspace_id)
    destination_info = read_api_connection_list(url_destination, workspace_id)
    except_col_list = ['execution_date', 'ab_id', 'ab_emitted_at', 'unique_id']

    # create yml schema files
    for dest in destination_info["destinations"]:
        if dest["workspaceId"] == workspace_id:
            dataset = dest.get("connectionConfiguration", {}).get("dataset_id", None)
            database = dest.get("connectionConfiguration", {}).get("project_id", None)
            destination_id = dest.get("destinationId")
            for i in api_request_json["connections"]:
                if i.get("destinationId") == destination_id and i.get("connectionId") == connection_id:
                    for k in i["syncCatalog"]["streams"]:
                        table_name = k["stream"]["name"]
                        col_list = []
                        new_table_name = convert_value_to_system_standart(table_name)

                        # create <model>.yml file
                        model_yml_dict, unique_key_list = create_yml_dict(
                            api_request_json, connection_id, table_name
                        )
                        create_yml_file(
                            model_yml_dict, "models/staging", f"stg_{new_table_name}"
                        )

                        for model in model_yml_dict['models']:
                            for col in model['columns']:
                                col_name = col.get('name')
                                if col_name not in except_col_list:
                                    col_list.append(col_name)

                        # create <model>.sql with unique_key
                        if unique_key_list:
                            create_model(table_name, new_table_name, unique_key_list=unique_key_list)
                        else:
                            create_model(table_name, new_table_name, col_list=col_list)

                    # create source schema yml file
                    source_yml = create_source_yml(
                        api_request_json, connection_id, database, dataset, destination_id
                    )
                    create_yml_file(source_yml, "models", "source")
