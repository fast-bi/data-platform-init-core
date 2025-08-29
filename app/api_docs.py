"""
API Documentation and configuration module for Fast.BI DBT Project Initialization API.
"""
from apiflask import APIFlask
from config import Config

def configure_api_docs(app: APIFlask):
    """
    Configure API documentation and OpenAPI specification for the application.
    
    Args:
        app: APIFlask application instance
    """
    # Set basic app description
    app.description = 'Fast.BI DBT Project Initialization API - A comprehensive solution for DBT project management and initialization'

    # Configure detailed API description
    app.config['DESCRIPTION'] = """
# DBT Project Initialization API

## Overview

The DBT Project Initialization API enables seamless creation and management of dbt (data build tool) projects across multiple environments. This API documentation describes available endpoints, request/response formats, and authentication requirements.

### Authentication

All endpoints require API key authentication. Include your API key in the request header:

```http
X-API-KEY: your_api_key_here
```

### Request/Response Format

All endpoints accept and return JSON data:

```http
Content-Type: application/json
Accept: application/json
```

## Core Components

### Kubernetes Pod Operator (K8S)

The K8S operator provides:
- Project initialization in Kubernetes clusters
- API-driven configuration
- Integration with Airflow KubernetesPodOperator
- Automated file compilation

### GKE Pod Operator

Google Cloud integration featuring:
- Native GKE cluster support
- Cloud-optimized performance
- Standard K8S feature parity
- Seamless Google Cloud integration

### API DBT Server Operator

Direct server integration offering:
- Dedicated API server communication
- Streamlined initialization process
- Real-time status updates
- Workflow automation

### Bash Operator

Development and testing features:
- Local execution support
- Shell script operations
- System-level access
- Development environment tools

## Operations Guide

### Project Creation

To create a new DBT project:

```http
POST /api/v3/projects
Content-Type: application/json
X-API-KEY: your_api_key_here

{
  "project_name": "example_project",
  "operator_type": "k8s",
  "configuration": {
    "warehouse_type": "snowflake",
    "environment": "production"
  }
}
```

### Project Listing

Retrieve all projects:

```http
GET /api/v3/projects?limit=25&offset=0
X-API-KEY: your_api_key_here
```

### Project Updates

Update project configuration:

```http
PATCH /api/v3/projects/{project_id}
Content-Type: application/json
X-API-KEY: your_api_key_here

{
  "configuration": {
    "schedule": "0 0 * * *"
  }
}
```

### Project Deletion

Remove a project:

```http
DELETE /api/v3/projects/{project_id}
X-API-KEY: your_api_key_here
```

## Error Handling

The API uses standard HTTP response codes:

- `200` - Success
- `201` - Created
- `400` - Bad Request
- `401` - Unauthorized
- `403` - Forbidden
- `404` - Not Found
- `500` - Internal Server Error

Error responses include detailed messages:

```json
{
  "error": {
    "code": "400",
    "message": "Invalid project configuration",
    "details": "Warehouse type is required"
  }
}
```

## Rate Limiting

API requests are limited to:
- 100 requests per minute per API key
- 1000 requests per hour per API key

## Support

For API support:
- Email: support@fast.bi
- Documentation: https://fast.bi/docs
- Status: https://status.fast.bi

## License

### Dual License

This API is available under both Apache 2.0 and MIT licenses:

**Apache 2.0**: [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

**MIT**: Permission granted for any use subject to the MIT License terms.

## Copyright

Copyright © 2024 Fast.BI. All rights reserved.
"""

    # Configure server environments
    app.config['SERVERS'] = [
        {
            'name': 'Production Server',
            'url': Config.DBT_INIT_API_LINK
        },
        {
            'name': 'Development Server',
            'url': 'http://localhost:8888'
        }
    ]

    # Configure contact information
    app.config['CONTACT'] = {
        'name': 'API Support',
        'url': 'https://fast.bi/support',
        'email': 'support@fast.bi'
    }

    # Configure license information
    app.config['LICENSE'] = {
        'name': 'Apache 2.0 and MIT',
        'url': 'https://fast.bi/licenses'
    }

    # Configure terms of service
    app.config['TERMS_OF_SERVICE'] = 'https://fast.bi/terms'

    # Configure API information
    app.config['INFO'] = {
        'title': 'Fast.BI DBT Project Initialization API',
        'version': '3.0',
        'description': 'API for DBT project management and initialization',
        'terms_of_service': 'https://fast.bi/terms',
        'contact': {
            'name': 'API Support',
            'url': 'https://fast.bi/support',
            'email': 'support@fast.bi'
        },
        'license': {
            'name': 'Apache 2.0 and MIT',
            'url': 'https://fast.bi/licenses'
        }
    }

    return app