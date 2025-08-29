# Data Platform Init Core

An API service for dbt project management and initialization in Fast.BI user console. Provides endpoints to bootstrap, configure, and manage dbt projects with opinionated templates and best practices.

## Overview

This image powers dbt project initialization and lifecycle management. It exposes REST APIs to scaffold new projects, manage configurations, generate boilerplate files, and apply curated templates for multiple data warehouse targets. It integrates with Fast.BI services to streamline onboarding and standardize project structure across teams.

## Architecture

### Core Components

- **Initialization API**: Endpoints for creating and configuring dbt projects (project init, profiles, packages, seeds).
- **Project Management API**: Endpoints for updating project settings, regenerating files, and applying upgrades.
- **Template Engine**: Opinionated templates for BigQuery, Snowflake, Redshift, and Fabric (macros and starter configs).
- **Docs & OpenAPI**: API docs served via APIFlask with Swagger UI at `/api/v3/docs`.
- **Security**: Token-based auth middleware for secured endpoints.

## Docker Image

### Base Image
- **Base**: Python 3.11.11-slim-bullseye

### Build

```bash
# Build the image
docker build -t data-platform-init-core .
```

### Environment Variables

- `PORT` - Service port (default: 8888)
- `DEBUG` - Enable debug logs (true/false)
- `DBT_DEFAULT_TARGET` - Default target profile (e.g., dev)
- `GIT_REPO_URL` - Optional, seed a repo as the base project
- `GIT_BRANCH` - Optional, branch to use for seeding

## Main Functionality

- **Project scaffolding**: Create new dbt projects with curated templates and macros
- **Profiles setup**: Generate `profiles.yml` for chosen warehouse
- **Template packs**: Apply warehouse-specific macros (cleanup, maintenance, etc.)
- **Docs & schemas**: Create baseline docs, schema.yml, and CI helpers
- **API docs**: Browse and try operations at `/api/v3/docs`

## Key API Areas

- `/api/v3/project/init` – Initialize a new dbt project
- `/api/v3/project/manage/*` – Manage project config, packages, profiles
- `/api/v3/docs` – OpenAPI documentation (Swagger UI)

## Health Checks

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' data-platform-init-core

# View health check logs
docker inspect --format='{{range .State.Health.Log}}{{.Output}}{{end}}' data-platform-init-core
```

## Troubleshooting

- **Profiles missing**: Ensure the correct warehouse target and credentials are provided
- **Template not applied**: Verify chosen template pack matches the warehouse
- **Git errors**: Check reachability and branch existence when seeding from a repo

## Getting Help

- **Documentation**: https://wiki.fast.bi
- **Issues**: https://github.com/fast-bi/data-platform-init-core/issues
- **Email**: support@fast.bi

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
