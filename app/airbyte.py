import os

import requests
from packaging import version


def get_airbyte_destination_version(connection_id):
    """
    Fetch the Airbyte destination version type (1 or 2) for a given connection ID.

    Returns:
        int: 1 for v1 destinations, 2 for v2 destinations, or None if an error occurs.
    """

    def make_post_request(url, payload, headers):
        """Helper function to perform a POST request and return the JSON response."""
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making POST request to {url}: {e}")
            return None

    base_url = 'http://' + os.environ.get("AIRBYTE_LOCAL_K8S_SVC_URL")
    headers = {"Content-Type": "application/json"}

    # Connection details
    connection_url = f"{base_url}/api/v1/connections/get/"
    connection_payload = {"connectionId": connection_id}
    connection_data = make_post_request(connection_url, connection_payload, headers)
    if not connection_data:
        print("Failed to fetch connection details.")
        return None

    destination_id = connection_data.get("destinationId")
    if not destination_id:
        print("Destination ID not found in connection details.")
        return None

    # Destination details
    destination_url = f"{base_url}/api/v1/destinations/get"
    destination_payload = {"destinationId": destination_id}
    destination_data = make_post_request(destination_url, destination_payload, headers)
    if not destination_data:
        print("Failed to fetch destination details.")
        return None

    destination_definition_id = destination_data.get("destinationDefinitionId")
    if not destination_definition_id:
        print("Destination definition ID not found in destination details.")
        return None

    # Destination definition details
    definition_url = f"{base_url}/api/v1/destination_definitions/get"
    definition_payload = {"destinationDefinitionId": destination_definition_id}
    definition_data = make_post_request(definition_url, definition_payload, headers)
    if not definition_data:
        print("Failed to fetch destination definition details.")
        return None

    docker_image_tag = definition_data.get("dockerImageTag")
    if not docker_image_tag:
        print("Docker image tag not found in destination definition details.")
        return None

    # Determine destination version
    try:
        parsed_docker_image_tag = version.parse(docker_image_tag)
        last_v1_version = version.parse("1.10.2")
        return 2 if parsed_docker_image_tag > last_v1_version else 1
    except Exception as e:
        print(f"Error parsing docker image tag version '{docker_image_tag}': {e}")
        return None
