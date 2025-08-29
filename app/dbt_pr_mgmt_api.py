from apiflask import APIFlask, Schema, abort, APIBlueprint
from apiflask.fields import Integer, String, Boolean, URL, DateTime, Raw, File, Nested, List
from apiflask.validators import Length, OneOf
from flask import request, jsonify
from security import auth
import git
import os
import shutil
import boto3
import yaml
import json
from datetime import datetime
import zipfile
from pathlib import Path
import subprocess
from config import Config  # make sure to import Config correctly based on your project structure
from minio import Minio
from minio.error import S3Error
import tempfile
import logging
import re
import requests
from typing import Dict, List, Optional, Tuple, Any
from marshmallow import validates, ValidationError
from pathlib import Path
import base64
from contextlib import contextmanager
from airbyte import get_airbyte_destination_version
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import SingleQuotedScalarString

# Schema Definitions
class ProjectDatesSchema(Schema):
    """Schema for project dates"""
    created_at = DateTime(required=True, metadata={'description': 'Project creation date from Git history'})
    last_modified = DateTime(required=True, metadata={'description': 'Last modification date from Git history'})

class ProjectListSchema(Schema):
    """Schema for project list response"""
    project_name = String(required=True, metadata={'description': 'Name of the DBT project'})
    path = String(required=True, metadata={'description': 'Project path in repository'})
    created_at = DateTime(required=True, metadata={'description': 'Project creation date from Git history'})
    last_modified = DateTime(required=True, metadata={'description': 'Last modification date from Git history'})

class ProjectVariablesSchema(Schema):
    project_name = String(required=True)
    variables = Raw(required=True, metadata={'description': 'Project variables from dbt_airflow_variables.yml'})

class UpdateVariablesInputSchema(Schema):
    project_name = String(required=True)
    variables = Raw(required=True)
    branch_name = String(required=True, validate=Length(1, 255))

class UpdateProfilesInputSchema(Schema):
    project_name = String(required=True)
    profiles = Raw(required=True)
    branch_name = String(required=True, validate=Length(1, 255))

class RefreshInputSchema(Schema):
    project_name = String(required=True, metadata={'description': 'Name of the DBT project'})
    airbyte_id = String(metadata={'description': 'Airbyte connection ID'})

class RenameInputSchema(Schema):
    new_project_name = String(required=True, metadata={'description': 'New name for the DBT project'})

    @validates('new_project_name')
    def validate_new_project_name(self, value):
        """Validate that new project name contains only lowercase letters, numbers, and underscores"""
        if not re.match(r'^[a-z0-9_]+$', value):
            raise ValidationError('Project name must contain only lowercase letters, numbers, and underscores')

class OwnerInputSchema(Schema):
    owner_name = String(required=True, metadata={'description': 'New Owner name of the DBT project'})

class PackageSchema(Schema):
    package = String(required=True, validate=Length(1, 255), metadata={'description': 'The DBT package name'})
    version = String(required=True, validate=Length(1, 16), metadata={'description': 'The DBT package version'})

class ProjectInfoSchema(Schema):
    """Schema for comprehensive project information"""
    project_name = String(required=True, metadata={'description': 'Name of the DBT project'})
    owner = String(required=True, metadata={'description': 'Project owner from DAG_OWNER variable'})
    airbyte_id = String(metadata={'description': 'Airbyte connection ID'})
    airbyte_workspace = String(metadata={'description': 'The Airbyte workspace ID'})
    airflow_dag_name = String(required=True, metadata={'description': 'Airflow DAG name'})
    repo_path = String(required=True, metadata={'description': 'Project path relative to repository root'})
    created_at = DateTime(required=True, metadata={'description': 'Project creation date from Git history'})
    last_modified = DateTime(required=True, metadata={'description': 'Last modification date from Git history'})
    data_quality_enabled = Boolean(required=True, metadata={'description': 'Data quality checks status'})
    data_quality_url = URL(metadata={'description': 'Data quality dashboard URL'})
    dbt_docs_enabled = Boolean(required=True, metadata={'description': 'DBT docs status'})
    dbt_docs_url = URL(metadata={'description': 'DBT documentation URL'})
    dbt_project_level = String(required=True, metadata={'description': 'The Data Governance Fabric group type.'})

class ProjectInfoOutputSchema(Schema):
    output = Raw(metadata={'description': 'The output of the DBT project information request.'})

class RefreshResponseSchema(Schema):
    success = Boolean(required=True, metadata={'description': 'Whether the refresh was successful'})
    message = String(required=True, metadata={'description': 'Status message or error details'})
    timestamp = DateTime(dump_default=datetime.now, metadata={'description': 'Time of refresh'})

class DeleteProjectParamsSchema(Schema):
    """Schema for project deletion parameters"""
    delete_data = Boolean(required=True, metadata={'description': 'Whether to delete project data from the warehouse'})
    delete_folder = Boolean(required=True, metadata={'description': 'Whether to delete the project folder from repository'})

class DeleteProjectResponseSchema(Schema):
    """Schema for project deletion response"""
    message = String(required=True, metadata={'description': 'Status message describing the deletion operation result'})
    project_name = String(required=True, metadata={'description': 'Name of the deleted project'})
    folder_deleted = Boolean(required=True, metadata={'description': 'Whether the project folder was successfully deleted'})
    data_deleted = Boolean(required=True, metadata={'description': 'Whether the warehouse data was successfully deleted'})
    timestamp = DateTime(dump_default=datetime.now, metadata={'description': 'Time of deletion operation'})

class ProjectManager:
    def __init__(self, repo_url: str, repo_token: str, base_path: str):
        self.repo_url = repo_url
        self.repo_token = repo_token
        self.base_path = base_path
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self._setup_logging()
        
        # Initialize MinIO client using config or environment variables
        minio_endpoint = Config.MINIO_ENDPOINT or os.getenv('MINIO_ENDPOINT')
        minio_access_key = Config.MINIO_ACCESS_KEY or os.getenv('MINIO_ACCESS_KEY')
        minio_secret_key = Config.MINIO_SECRET_KEY or os.getenv('MINIO_SECRET_KEY')
        
        if not all([minio_endpoint, minio_access_key, minio_secret_key]):
            raise ValueError("MinIO configuration is incomplete. Please check MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY.")
        
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=True  # Use HTTPS
        )
        self.repo = None
        self._init_repo()

    def _setup_logging(self):
        """Configure logging with appropriate levels"""
        # Set git logger to WARNING to suppress debug messages
        git_logger = logging.getLogger('git')
        git_logger.setLevel(logging.WARNING)
        
        # Configure our logger
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _init_repo(self):
        """Initialize or refresh Git repository with better error handling"""
        repo_path = Path(self.base_path) / 'dbt_projects'
        
        try:
            if repo_path.exists():
                self.logger.info("Repository exists, pulling latest changes")
                self.repo = git.Repo(repo_path)
                self.repo.remotes.origin.pull()
            else:
                self.logger.info("Cloning repository")
                repo_url_with_token = self.repo_url.replace(
                    "https://", f"https://oauth2:{self.repo_token}@"
                )
                self.repo = git.Repo.clone_from(repo_url_with_token, repo_path)
                
        except git.GitCommandError as e:
            self.logger.error(f"Git command failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize repository: {e}")
            raise

    def list_projects(self) -> List[Dict]:
        """
        List all DBT projects in repository with Git-based creation and modification dates
        
        Returns:
            List of dictionaries containing project information:
                - project_name: Name of the project
                - path: Project path relative to repository root
                - created_at: Project creation date from Git history
                - last_modified: Last modification date from Git history
        """
        projects = []
        repo_path = Path(self.base_path) / 'dbt_projects'
        
        if not repo_path.exists():
            self.logger.error(f"Repository path {repo_path} does not exist")
            return []

        try:
            for item in repo_path.iterdir():
                if item.is_dir() and (item / 'dbt_project.yml').exists():
                    # Get project path relative to repo root for Git operations
                    relative_path = item.relative_to(repo_path)
                    
                    # Get dates from Git history
                    created_at, last_modified = self.get_project_dates(str(relative_path))
                    
                    if created_at and last_modified:
                        projects.append({
                            'project_name': item.name,
                            'path': str(relative_path),
                            'created_at': created_at,
                            'last_modified': last_modified
                        })
                    else:
                        # Fallback to filesystem dates if Git history unavailable
                        self.logger.warning(f"Using filesystem dates for {item.name}")
                        projects.append({
                            'project_name': item.name,
                            'path': str(relative_path),
                            'created_at': datetime.fromtimestamp(item.stat().st_ctime),
                            'last_modified': datetime.fromtimestamp(item.stat().st_mtime)
                        })

            return sorted(projects, key=lambda x: x['created_at'], reverse=True)

        except Exception as e:
            self.logger.error(f"Error listing projects: {str(e)}")
            raise

    def delete_project_dependencies(self, project_name: str) -> Dict[str, bool]:
        """
        Delete all project dependencies
        
        Args:
            project_name: Name of the project
            
        Returns:
            Dict[str, bool]: Status of each dependency deletion
        """
        dependency_manager = ProjectDependencyManager(base_path=self.base_path)
        
        status = {
            'airflow_deleted': False,
            'data_catalog_deleted': False,
            'data_quality_deleted': False
        }
        
        # Create metadata manager instance to share
        metadata_manager = ProjectMetadataManager(base_path=self.base_path)
        
        # Delete Airflow DAG and files
        status['airflow_deleted'] = dependency_manager.delete_airflow_dag(
            project_name,
            metadata_manager=metadata_manager
        )
        
        # Delete Data Catalog entry
        status['data_catalog_deleted'] = dependency_manager.delete_data_catalog(project_name)
        
        # Delete Data Quality entry
        status['data_quality_deleted'] = dependency_manager.delete_data_quality(project_name)
        
        return status

    def delete_project(self, project_name: str, delete_data: bool = False, 
                    delete_folder: bool = True) -> Tuple[bool, Dict[str, bool]]:
        """Delete project folder and/or data
        
        Args:
            project_name: Name of the DBT project
            delete_data: Whether to delete data from the warehouse
            delete_folder: Whether to delete the project folder
        
        Returns:
            Tuple[bool, Dict[str, bool]]: (success, deletion_status)
            deletion_status contains flags for what was actually deleted
        """
        project_path = Path(self.base_path) / 'dbt_projects' / project_name
        
        if not project_path.exists():
            self.logger.error(f"Project path {project_path} does not exist")
            return False, {}

        deletion_status = {
            'folder_deleted': False,
            'data_deleted': False
        }

        if delete_data:
            try:
                # Initialize warehouse auth manager
                auth_manager = WarehouseAuthManager(self.logger)
                
                # Read profiles.yml to determine warehouse type
                profiles_path = project_path / 'profiles.yml'
                if not profiles_path.exists():
                    self.logger.error(f"profiles.yml not found in project {project_name}")
                    return False, deletion_status

                with open(profiles_path, 'r') as f:
                    profiles = yaml.safe_load(f)
                    first_profile = next(iter(profiles.values()))
                    target = first_profile.get('target')
                    outputs = first_profile.get('outputs', {})
                    target_config = outputs.get(target, {})
                    warehouse_type = target_config.get('type')

                # Define cleanup macro paths for different warehouses
                MACRO_PATHS = {
                    'bigquery': '/usr/src/app/assets/dbt/macros/bigquery_cleanup_dbt_dataset.sql',
                    'redshift': '/usr/src/app/assets/dbt/macros/redshift_cleanup_dbt_dataset.sql',
                    'snowflake': '/usr/src/app/assets/dbt/macros/snowflake_cleanup_dbt_dataset.sql',
                    'fabric': '/usr/src/app/assets/dbt/macros/fabric_cleanup_dbt_dataset.sql',
                    'postgres': '/usr/src/app/assets/dbt/macros/postgres_cleanup_dbt_dataset.sql'
                }

                if warehouse_type not in MACRO_PATHS:
                    self.logger.error(f"Unsupported warehouse type: {warehouse_type}")
                    return False, deletion_status

                # Set up macros directory
                macros_path = project_path / 'macros'
                if not macros_path.exists():
                    macros_path.mkdir(parents=True)

                # Copy appropriate cleanup macro
                cleanup_macro_path = macros_path / 'cleanup_dbt_dataset.sql'
                source_macro = MACRO_PATHS[warehouse_type]

                if not os.path.exists(source_macro):
                    self.logger.error(f"Cleanup macro not found at {source_macro}")
                    return False, deletion_status

                shutil.copy(source_macro, cleanup_macro_path)

                # Get the appropriate auth handler
                auth_handlers = {
                    'bigquery': auth_manager.bigquery_auth,
                    'snowflake': auth_manager.snowflake_auth,
                    'redshift': auth_manager.redshift_auth,
                    'fabric': auth_manager.fabric_auth,
                    'postgres': auth_manager.postgres_auth
                }

                auth_handler = auth_handlers.get(warehouse_type)
                if not auth_handler:
                    self.logger.error(f"No auth handler found for warehouse type: {warehouse_type}")
                    return False, deletion_status

                # Execute dbt commands within the auth context
                with auth_handler():
                    try:
                        # Run dbt deps
                        deps_result = subprocess.run(
                            ['dbt', 'deps'],
                            cwd=project_path,
                            capture_output=True,
                            text=True,
                            check=True
                        )
                        self.logger.debug(f"DBT deps output: {deps_result.stdout}")

                        # Run cleanup operation
                        cleanup_result = subprocess.run(
                            ['dbt', 'run-operation', 'cleanup_dbt_dataset',
                            '--args', '{"dry_run": false}',
                            '--target', 'sa'],
                            cwd=project_path,
                            capture_output=True,
                            text=True,
                            check=True
                        )
                        self.logger.debug(f"DBT cleanup output: {cleanup_result.stdout}")
                        deletion_status['data_deleted'] = True

                    except subprocess.CalledProcessError as e:
                        self.logger.error(f"DBT command failed: {e.stderr}")
                        return False, deletion_status

            except Exception as e:
                self.logger.error(f"Error during data deletion: {str(e)}")
                return False, deletion_status

        if delete_folder:
            try:
                # First delete dependencies
                dependency_status = self.delete_project_dependencies(project_name)
                
                # Generate branch name
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                branch_name = f"del_{project_name}_{timestamp}"
                
                # Then delete the project folder
                shutil.rmtree(project_path)
                self._commit_and_push(f"Deleted project {project_name}", branch=branch_name)
                
                # Update deletion status
                deletion_status['folder_deleted'] = True
                deletion_status.update(dependency_status)
                deletion_status['branch_name'] = branch_name  # Add branch name to status
                
            except Exception as e:
                self.logger.error(f"Error deleting project folder: {str(e)}")
                return False, deletion_status

        return True, deletion_status

    def archive_project(self, project_name: str, bucket_name: str = None) -> str:
        """
        Archive project to MinIO
        
        Args:
            project_name: Name of the project to archive
            bucket_name: MinIO bucket name (defaults to MINIO_BUCKET_NAME from config/env)
            
        Returns:
            str: MinIO object key
            
        Raises:
            FileNotFoundError: If project doesn't exist
            S3Error: If MinIO operations fail
        """
        # Get bucket name from config/env if not provided
        bucket_name = bucket_name or Config.MINIO_BUCKET_NAME or os.getenv('MINIO_BUCKET_NAME', 'dbt-project-archive')
        project_path = Path(self.base_path) / 'dbt_projects' / project_name
        
        if not project_path.exists():
            raise FileNotFoundError(f"Project {project_name} not found")

        # Create a temporary file for the zip
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_zip:
            temp_zip_path = Path(temp_zip.name)
            
            # Create zip archive
            with zipfile.ZipFile(temp_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(project_path):
                    for file in files:
                        file_path = Path(root) / file
                        arcname = file_path.relative_to(project_path)
                        zipf.write(file_path, arcname)
            
            try:
                # Ensure bucket exists
                if not self.minio_client.bucket_exists(bucket_name):
                    self.minio_client.make_bucket(bucket_name)
                
                # Generate MinIO object key
                minio_key = f"archived_projects/{project_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                
                # Upload to MinIO
                self.minio_client.fput_object(
                    bucket_name,
                    minio_key,
                    str(temp_zip_path)
                )
                
                return minio_key
                
            except S3Error as e:
                raise Exception(f"Failed to upload to MinIO: {str(e)}")
            
            finally:
                # Clean up temporary zip file
                temp_zip_path.unlink()

    def get_project_variables(self, project_name: str) -> Dict:
        """Get project variables from dbt_airflow_variables.yml"""
        variables_path = Path(self.base_path) / 'dbt_projects' / project_name / 'dbt_airflow_variables.yml'
        
        if not variables_path.exists():
            return {}

        with open(variables_path, 'r') as f:
            return yaml.safe_load(f)
        
    def get_project_profiles(self, project_name: str) -> Dict:
        """Get project variables from profiles.yml"""
        variables_path = Path(self.base_path) / 'dbt_projects' / project_name / 'profiles.yml'
        
        if not variables_path.exists():
            return {}

        with open(variables_path, 'r') as f:
            return yaml.safe_load(f)

    def update_project_variables(self, project_name: str, variables: Dict, 
                               branch_name: str) -> bool:
        """Update project variables and create new branch"""
        project_path = Path(self.base_path) / 'dbt_projects' / project_name
        variables_path = project_path / 'dbt_airflow_variables.yml'

        if not project_path.exists():
            return False

        # Create and checkout new branch
        current = self.repo.create_head(branch_name)
        current.checkout()

        yaml = YAML()
        yaml.preserve_quotes = True

        # Load existing variables
        if variables_path.exists():
            with open(variables_path, 'r') as f:
                existing_data = yaml.load(f) or {}
        else:
            existing_data = {}

        # Recursively apply single quotes to all values in a dictionary
        def apply_single_quotes_to_values(data):
            if isinstance(data, dict):
                return {k: apply_single_quotes_to_values(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [apply_single_quotes_to_values(i) for i in data]
            else:
                return SingleQuotedScalarString(str(data))

        # Merge variables
        for key, value in variables.items():
            existing_data[key] = value

        # Apply single quotes to the merged data values
        updated_data = apply_single_quotes_to_values(existing_data)

        # Update variables file
        with open(variables_path, 'w') as f:
            yaml.dump(updated_data, f)

        self._commit_and_push(
            f"Updated variables for project {project_name}",
            branch_name
        )
        
        return True


    def update_project_profiles(self, project_name: str, variables: Dict, 
                               branch_name: str) -> bool:
        """Update project profiles and create new branch"""
        project_path = Path(self.base_path) / 'dbt_projects' / project_name
        profiles_path = project_path / 'profiles.yml'

        if not project_path.exists():
            return False

        # Create and checkout new branch
        current = self.repo.create_head(branch_name)
        current.checkout()

        # Update variables file
        with open(profiles_path, 'w') as f:
            yaml.safe_dump(variables, f)

        self._commit_and_push(
            f"Updated profiles.yml for project {project_name}",
            branch_name
        )
        
        return True

    def _get_default_branch(self) -> str:
        """
        Detect the default branch name of the repository
        
        Returns:
            str: Name of the default branch (e.g., 'main', 'master', 'prod')
        """
        try:
            # Method 1: Try to get the branch HEAD points to
            default_branch = self.repo.git.rev_parse("--abbrev-ref", "origin/HEAD")
            if default_branch.startswith("origin/"):
                return default_branch[7:]  # Remove 'origin/' prefix
                
        except git.GitCommandError:
            try:
                # Method 2: Get the branch that HEAD points to on origin
                remote_refs = self.repo.git.ls_remote("--symref", "origin", "HEAD").split("\n")
                for line in remote_refs:
                    if line.startswith("ref:"):
                        match = re.search(r"refs/heads/(\S+)\s+", line)
                        if match:
                            return match.group(1)
                            
            except git.GitCommandError:
                try:
                    # Method 3: Try to find the default branch from local branches
                    for branch in self.repo.branches:
                        if branch.name in ['main', 'master', 'prod']:
                            return branch.name
                            
                    # If we get here and haven't found a default branch,
                    # return the current branch name
                    return self.repo.active_branch.name
                    
                except git.GitCommandError as e:
                    self.logger.error(f"Failed to detect default branch: {e}")
                    raise
                    
        # If all methods fail, fallback to 'main'
        self.logger.warning("Could not detect default branch, falling back to 'main'")
        return 'main'

    def _commit_and_push(self, message: str, branch: str = None):
            """
            Commit and push changes to repository with proper branch management
            
            Args:
                message (str): Commit message
                branch (str, optional): Target branch name. If None, uses detected default branch
            """
            try:
                git_logger = logging.getLogger('git')
                original_level = git_logger.level
                git_logger.setLevel(logging.WARNING)  # Temporarily suppress git debug messages
                
                try:
                    # Get default branch for returning later
                    default_branch = self._get_default_branch()
                    original_branch = self.repo.active_branch.name
                    
                    # If branch is not specified, use the default branch
                    if branch is None:
                        branch = default_branch
                        
                    # Ensure we're on the correct branch
                    if original_branch != branch:
                        try:
                            # Try to checkout existing branch
                            self.repo.git.checkout(branch)
                        except git.GitCommandError:
                            # If branch doesn't exist locally, create it
                            self.repo.git.checkout('-b', branch)
                    
                    # Add, commit and push changes
                    self.repo.git.add(A=True)
                    self.repo.index.commit(message)
                    
                    # Push with branch specification
                    self.repo.git.push('origin', f'{branch}:{branch}')
                    
                    self.logger.info(f"Successfully committed and pushed to {branch}: {message}")
                    
                    # Always return to default branch
                    if branch != default_branch:
                        self.repo.git.checkout(default_branch)
                        # Pull latest changes on default branch
                        self.repo.remotes.origin.pull(default_branch)
                        
                finally:
                    git_logger.setLevel(original_level)  # Restore original log level
                    
            except git.GitCommandError as e:
                self.logger.error(f"Git command failed during commit/push: {e}")
                # Try to return to default branch even if there was an error
                try:
                    if self.repo.active_branch.name != default_branch:
                        self.repo.git.checkout(default_branch)
                        self.repo.remotes.origin.pull(default_branch)
                except Exception as checkout_error:
                    self.logger.error(f"Failed to return to default branch: {checkout_error}")
                raise
            except Exception as e:
                self.logger.error(f"Unexpected error during commit/push: {e}")
                # Try to return to default branch even if there was an error
                try:
                    if self.repo.active_branch.name != default_branch:
                        self.repo.git.checkout(default_branch)
                        self.repo.remotes.origin.pull(default_branch)
                except Exception as checkout_error:
                    self.logger.error(f"Failed to return to default branch: {checkout_error}")
                raise

    def refresh_repository(self) -> Dict[str, bool]:
        """
        Force refresh the git repository by pulling latest changes
        
        Returns:
            Dict with status and message
        """
        try:
            self.logger.info("Starting repository refresh")
            if self.repo is None:
                self.logger.info("Repository not initialized, running init")
                self._init_repo()
                return {
                    "success": True,
                    "message": "Repository initialized successfully"
                }
            
            # Check if repo path exists
            repo_path = Path(self.base_path) / 'dbt_projects'
            if not repo_path.exists():
                self.logger.warning("Repository path does not exist, reinitializing")
                self._init_repo()
                return {
                    "success": True,
                    "message": "Repository reinitialized successfully"
                }

            # Fetch and pull latest changes
            self.logger.info("Fetching latest changes")
            origin = self.repo.remotes.origin
            origin.fetch()
            
            # Store current branch
            current_branch = self.repo.active_branch.name
            
            # Pull latest changes
            self.logger.info(f"Pulling latest changes on branch {current_branch}")
            pull_info = origin.pull(current_branch)
            
            # Check pull results
            if pull_info:
                summary = pull_info[0].note
                self.logger.info(f"Pull completed: {summary}")
                return {
                    "success": True,
                    "message": f"Repository refreshed successfully: {summary}"
                }
            
            return {
                "success": True,
                "message": "Repository is already up to date"
            }
            
        except git.GitCommandError as e:
            error_msg = f"Git command failed during refresh: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg
            }
        except Exception as e:
            error_msg = f"Unexpected error during refresh: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "message": error_msg
            }

    def get_project_dates(self, project_path: str) -> tuple[Optional[datetime], Optional[datetime]]:
        """
        Get project creation and last modified dates from Git history
        
        Args:
            project_path: Path to the project relative to repository root
            
        Returns:
            Tuple of (creation_date, last_modified_date)
        """
        try:
            # Get project creation date (first commit)
            creation_logs = self.repo.git.log(
                '--diff-filter=A',  # Only addition events
                '--format=%ad',     # Only date
                '--date=iso',       # ISO format
                '--', project_path
            ).splitlines()
            
            creation_date = (
                datetime.fromisoformat(creation_logs[-1].rsplit(' ', 1)[0])
                if creation_logs else None
            )

            # Get last modified date (most recent commit)
            modified_logs = self.repo.git.log(
                '-1',               # Most recent commit
                '--format=%ad',     # Only date
                '--date=iso',       # ISO format
                '--', project_path
            ).splitlines()
            
            last_modified = (
                datetime.fromisoformat(modified_logs[0].rsplit(' ', 1)[0])
                if modified_logs else None
            )

            return creation_date, last_modified

        except git.exc.GitError as e:
            self.logger.error(f"Failed to get Git history for {project_path}: {str(e)}")
            return None, None

    def update_project_owner(self, project_name: str, new_owner: str, branch_name: str = None) -> Dict[str, Any]:
        """
        Update the project owner in variables file
        
        Args:
            project_name: Name of the project
            new_owner: New owner name to set
            branch_name: Optional branch name (will be auto-generated if not provided)
            
        Returns:
            Dict containing status and branch information
        """
        try:
            # Generate branch name if not provided
            if branch_name is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                branch_name = f"updt_owner_{project_name}_{timestamp}"

            # Get current variables
            current_variables = self.get_project_variables(project_name)
            if not current_variables:
                return {
                    'success': False,
                    'message': f"Variables not found for project {project_name}"
                }

            # Update owner in all configurations
            for config in current_variables.values():
                if isinstance(config, dict):
                    config['DAG_OWNER'] = new_owner

            # Update variables file with new owner
            success = self.update_project_variables(
                project_name=project_name,
                variables=current_variables,
                branch_name=branch_name
            )

            if not success:
                return {
                    'success': False,
                    'message': f"Failed to update variables for project {project_name}"
                }

            # Get repository web URL for the branch
            repo_url = self._get_repo_web_url()
            if repo_url:
                branch_url = f"{repo_url.rstrip('/')}/tree/{branch_name}"
            else:
                branch_url = None

            return {
                'success': True,
                'message': f"Successfully updated owner for project {project_name}",
                'branch_name': branch_name,
                'branch_url': branch_url,
                'project_name': project_name,
                'new_owner': new_owner
            }

        except Exception as e:
            self.logger.error(f"Error updating project owner: {str(e)}")
            return {
                'success': False,
                'message': f"Failed to update owner: {str(e)}"
            }

    def _get_repo_web_url(self) -> Optional[str]:
        """
        Get web URL for the repository (without branch/path information)
        
        Returns:
            Optional[str]: Base web URL for the repository
        """
        try:
            remote_url = self.repo.remotes.origin.url
            
            # Handle HTTPS URLs with token
            if 'oauth2:' in remote_url:
                url = remote_url.split('oauth2:')[1].split('@', 1)[1]
            # Handle SSH URLs
            elif '@' in remote_url:
                url = remote_url.split('@')[1].replace(':', '/')
            # Handle HTTPS URLs without token
            else:
                url = remote_url.split('://', 1)[1]

            # Remove .git suffix if present
            url = url.replace('.git', '')
            
            # Ensure URL starts with https://
            if not url.startswith('https://'):
                url = f'https://{url}'
                
            return url

        except Exception as e:
            self.logger.error(f"Failed to get repository web URL: {str(e)}")
            return None

    def rename_project(self, old_project_name: str, new_project_name: str, branch_name: str = None) -> Dict[str, Any]:
        """
        Rename the project using a temporary location to avoid conflicts
        
        Process:
        1. Copy project to temp location
        2. Update project files in temp location
        3. Delete old project from master and its dependencies
        4. Create new branch from clean master
        5. Copy renamed project from temp to repo
        6. Commit and push changes
        
        Args:
            old_project_name: Current project name
            new_project_name: New project name
            branch_name: Optional branch name (will be auto-generated if not provided)
            
        Returns:
            Dict containing status and branch information
        """
        try:
            # Generate branch name if not provided
            if branch_name is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                branch_name = f"ren_proj_to_{new_project_name}_{timestamp}"

            project_path = Path(self.base_path) / 'dbt_projects' / old_project_name
            if not project_path.exists():
                return {
                    'success': False,
                    'message': f"Project {old_project_name} not found"
                }

            # Create temporary directory
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_project_path = Path(temp_dir) / old_project_name
                
                try:
                    # 1. Copy project to temp location
                    shutil.copytree(project_path, temp_project_path)
                    
                    # 2. Update project files in temp location
                    self._update_variables_file(temp_project_path, old_project_name, new_project_name)
                    self._update_dbt_project_file(temp_project_path, old_project_name, new_project_name)
                    self._update_profiles_file(temp_project_path, old_project_name, new_project_name)
                    
                    # Rename temp project directory to new name
                    temp_new_project_path = temp_project_path.parent / new_project_name
                    temp_project_path.rename(temp_new_project_path)
                    
                    # 3. Delete old project and dependencies from master
                    success, deletion_status = self.delete_project(
                        old_project_name,
                        delete_data=False,
                        delete_folder=True
                    )
                    
                    if not success:
                        raise Exception(f"Failed to clean up old project: {deletion_status}")
                    
                    # Make sure we're on default branch and it's up to date
                    default_branch = self._get_default_branch()
                    self.repo.git.checkout(default_branch)
                    self.repo.remotes.origin.pull()
                    
                    # 4. Create new branch from clean master
                    current = self.repo.create_head(branch_name)
                    current.checkout()
                    
                    # 5. Copy renamed project from temp to repo
                    new_project_path = Path(self.base_path) / 'dbt_projects' / new_project_name
                    shutil.copytree(temp_new_project_path, new_project_path)
                    
                    # 6. Commit and push changes
                    self._commit_and_push(
                        f"Rename project from {old_project_name} to {new_project_name}",
                        branch_name
                    )
                    
                    # Get repository web URL for the branch
                    repo_url = self._get_repo_web_url()
                    branch_url = f"{repo_url}/tree/{branch_name}" if repo_url else None
                    
                    return {
                        'success': True,
                        'message': f"Successfully renamed project from {old_project_name} to {new_project_name}",
                        'branch_name': branch_name,
                        'branch_url': branch_url,
                        'old_project_name': old_project_name,
                        'new_project_name': new_project_name,
                        'cleanup_status': deletion_status
                    }

                except Exception as e:
                    # If anything fails, ensure we're back on default branch
                    try:
                        default_branch = self._get_default_branch()
                        self.repo.git.checkout(default_branch)
                        self.repo.remotes.origin.pull()
                    except Exception as checkout_error:
                        self.logger.error(f"Failed to return to default branch: {checkout_error}")
                    raise Exception(f"Failed during rename process: {str(e)}")

        except Exception as e:
            self.logger.error(f"Failed to rename project: {str(e)}")
            return {
                'success': False,
                'message': f"Failed to rename project: {str(e)}"
            }

    def _update_variables_file(self, project_path: Path, old_name: str, new_name: str):
            """
            Update dbt_airflow_variables.yml file with support for different operator prefixes
            """
            variables_path = project_path / 'dbt_airflow_variables.yml'
            if not variables_path.exists():
                raise FileNotFoundError(f"Variables file not found at {variables_path}")

            with open(variables_path, 'r') as f:
                content = yaml.safe_load(f)

            if not content:
                raise ValueError("Empty or invalid variables file")

            # Find the key that ends with the old project name
            old_name_upper = old_name.upper()
            current_key = None
            for key in content.keys():
                if key.endswith(f"_DBT_PRJ_{old_name_upper}"):
                    current_key = key
                    break

            if not current_key:
                raise ValueError(f"Could not find configuration key for project {old_name}")

            # Extract the prefix (BASH_, API_, GKE_, K8S_, etc.)
            prefix = current_key[:current_key.index("_DBT_PRJ_") + 1]  # Include the trailing underscore
            
            # Create new key with same prefix but new project name
            new_key = f"{prefix}DBT_PRJ_{new_name.upper()}"
            
            # Update the key while preserving the content
            content[new_key] = content.pop(current_key)
            
            # Update values containing the project name
            for key in ['DAG_ID', 'DBT_PROJECT_NAME', 'DBT_PROJECT_DIRECTORY', 'MANIFEST_NAME']:
                if key in content[new_key]:
                    content[new_key][key] = content[new_key][key].replace(old_name, new_name)

            # Save the updated content
            with open(variables_path, 'w') as f:
                yaml.safe_dump(content, f, sort_keys=False)

    def _update_dbt_project_file(self, project_path: Path, old_name: str, new_name: str):
        """Update dbt_project.yml file"""
        project_file_path = project_path / 'dbt_project.yml'
        if not project_file_path.exists():
            raise FileNotFoundError(f"Project file not found at {project_file_path}")

        with open(project_file_path, 'r') as f:
            content = yaml.safe_load(f)

        # Update project name
        content['name'] = new_name
        content['profile'] = new_name

        # Update models section
        if 'models' in content and old_name in content['models']:
            content['models'][new_name] = content['models'].pop(old_name)

        with open(project_file_path, 'w') as f:
            yaml.safe_dump(content, f, sort_keys=False)

    def _update_profiles_file(self, project_path: Path, old_name: str, new_name: str):
        """Update profiles.yml file"""
        profiles_path = project_path / 'profiles.yml'
        if not profiles_path.exists():
            raise FileNotFoundError(f"Profiles file not found at {profiles_path}")

        with open(profiles_path, 'r') as f:
            content = yaml.safe_load(f)

        # Update profile name
        if old_name in content:
            content[new_name] = content.pop(old_name)

        with open(profiles_path, 'w') as f:
            yaml.safe_dump(content, f, sort_keys=False)

    def compile_dbt_manifest(self, project_name: str) -> Optional[Path]:
        """
        Compile DBT manifest file for a project if target folder doesn't exist
        
        Args:
            project_name: Name of the DBT project
            
        Returns:
            Path: Path to manifest.json if successful, None if failed 
        """
        project_path = self.base_path / 'dbt_projects' / project_name
        target_path = project_path / 'target'
        manifest_path = target_path / 'manifest.json'
        
        if not project_path.exists():
            self.logger.error(f"Project path {project_path} does not exist")
            return None
            
        # Return existing manifest if target folder exists
        if target_path.exists() and manifest_path.exists():
            self.logger.info(f"Using existing manifest for {project_name}")
            return manifest_path
            
        try:
            self.logger.info(f"Compiling new manifest for {project_name}")
            
            # Initialize warehouse auth manager with project path
            auth_manager = WarehouseAuthManager(self.logger, str(project_path))
            
            # Execute dbt commands within the auth context
            with auth_manager.get_auth_context():
                # Run dbt deps
                deps_result = subprocess.run(
                    ['dbt', 'deps', '--quiet'],
                    cwd=project_path,
                    check=True,
                    capture_output=True,
                    text=True
                )
                self.logger.debug(f"dbt deps output: {deps_result.stdout}")
                
                # Run dbt compile
                compile_result = subprocess.run(
                    ['dbt', 'compile', '--quiet'],
                    cwd=project_path,
                    check=True,
                    capture_output=True,
                    text=True
                )
                self.logger.debug(f"dbt compile output: {compile_result.stdout}")
            
            if manifest_path.exists():
                self.logger.info(f"Successfully compiled manifest for {project_name}")
                return manifest_path
                
            self.logger.error(f"Manifest file not generated at {manifest_path}")
            return None
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"DBT command failed for {project_name}: {e.stderr}")
            return None
        except Exception as e:
            self.logger.error(f"Error compiling manifest for {project_name}: {str(e)}")
            return None

    def clean_dbt_project(self, project_name: str) -> bool:
        """
        Clean DBT project target folder using 'dbt clean' command
        
        Args:
            project_name: Name of the DBT project
            
        Returns:
            bool: True if cleaning was successful, False otherwise
        """
        project_path = self.base_path / 'dbt_projects' / project_name
        target_path = project_path / 'target'
        
        if not project_path.exists():
            self.logger.error(f"Project path {project_path} does not exist")
            return False
            
        # Only run clean if target folder exists
        if not target_path.exists():
            self.logger.info(f"No target folder found for {project_name}, skipping clean")
            return True
                
        try:
            self.logger.info(f"Cleaning project {project_name}")
            result = subprocess.run(['dbt', 'clean', '--quiet'],
                            cwd=project_path,
                            check=True,
                            capture_output=True,
                            text=True)
            
            # Verify target folder was removed
            if not target_path.exists():
                self.logger.info(f"Successfully cleaned project {project_name}")
                return True
                
            self.logger.error(f"Target folder still exists after clean for {project_name}")
            return False
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"DBT clean command failed for {project_name}: {e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"Error cleaning project {project_name}: {str(e)}")
            return False

class ProjectMetadataManager:
    def __init__(self, base_path: str, repo: git.Repo = None):
        self.base_path = Path(base_path)
        self.repo = repo
        self.customer = Config.CUSTOMER or os.getenv('CUSTOMER')
        self.domain = Config.DOMAIN or os.getenv('DOMAIN')
        if not self.customer:
            raise ValueError("Environment variable CUSTOMER is required")

        # Set up logging
        self.logger = logging.getLogger(__name__)
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging with appropriate levels"""
        # Set git logger to WARNING to suppress debug messages
        git_logger = logging.getLogger('git')
        git_logger.setLevel(logging.WARNING)
        
        # Configure our logger
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _construct_url(self, project_name: str, url_type: str) -> str:
        """
        Construct standardized URLs for data quality and docs
        
        Args:
            project_name: Name of the project
            url_type: Either 'quality' or 'docs'
            
        Returns:
            Constructed URL string
        """
        # Convert project name to URL-friendly format
        url_project_name = project_name.replace('_', '-').lower()
        
        if url_type == 'quality':
            return f"https://{self.customer}.{self.domain}/data-quality/live/data-quality.{self.customer}.{self.domain}/{url_project_name}/%23/alerts"
        elif url_type == 'docs':
            return f"https://{self.customer}.{self.domain}/data-catalog/live/data-catalog.{self.customer}.{self.domain}/{url_project_name}/%23!/overview"
        else:
            raise ValueError(f"Invalid URL type: {url_type}")

    def _read_variables_file(self, project_name: str) -> Dict:
        """Read and parse dbt_airflow_variables.yml file"""
        variables_path = self.base_path / 'dbt_projects' / project_name / 'dbt_airflow_variables.yml'
        if not variables_path.exists():
            return {}
        
        try:
            with open(variables_path, 'r') as f:
                yaml_content = yaml.safe_load(f)
                # If file is empty or not a dict
                if not yaml_content or not isinstance(yaml_content, dict):
                    return {}
                
                # Get the first (and typically only) top-level configuration
                # Since the top-level key is dynamic, we just need the values
                first_config = next(iter(yaml_content.values()), {})
                return first_config
        except Exception as e:
            logging.error(f"Error reading variables file for project {project_name}: {str(e)}")
            return {}
        
    def _read_packages_file(self, project_name: str) -> List[Dict]:
        """
        Read and parse packages.yml file
        
        Args:
            project_name: Name of the DBT project
            
        Returns:
            List[Dict]: List of package configurations with name and version
        """
        packages_path = self.base_path / 'dbt_projects' / project_name / 'packages.yml'
        if not packages_path.exists():
            self.logger.warning(f"packages.yml not found for project {project_name}")
            return []
        
        try:
            with open(packages_path, 'r') as f:
                yaml_content = yaml.safe_load(f)
                
                # Check if file is empty or doesn't have packages key
                if not yaml_content or not isinstance(yaml_content, dict) or 'packages' not in yaml_content:
                    self.logger.warning(f"No packages defined in {packages_path}")
                    return []
                
                # Extract package information
                packages = []
                for pkg in yaml_content['packages']:
                    if isinstance(pkg, dict) and 'package' in pkg and 'version' in pkg:
                        packages.append({
                            'package': pkg['package'],
                            'version': str(pkg['version'])  # Convert version to string
                        })
                
                return packages
                
        except Exception as e:
            self.logger.error(f"Error reading packages file for project {project_name}: {str(e)}")
            return []

    def _get_git_dates(self, project_path: str) -> tuple[Optional[datetime], Optional[datetime]]:
        """
        Get project creation and last modified dates from Git history
        
        Args:
            project_path: Path to the project relative to repository root
            
        Returns:
            Tuple of (creation_date, last_modified_date)
        """
        try:
            if not self.repo:
                self.logger.warning("Git repository not initialized, using filesystem dates")
                return None, None

            # Get project creation date (first commit)
            creation_logs = self.repo.git.log(
                '--diff-filter=A',  # Only addition events
                '--format=%ad',     # Only date
                '--date=iso',       # ISO format
                '--', project_path
            ).splitlines()
            
            creation_date = (
                datetime.fromisoformat(creation_logs[-1].rsplit(' ', 1)[0])
                if creation_logs else None
            )

            # Get last modified date (most recent commit)
            modified_logs = self.repo.git.log(
                '-1',               # Most recent commit
                '--format=%ad',     # Only date
                '--date=iso',       # ISO format
                '--', project_path
            ).splitlines()
            
            last_modified = (
                datetime.fromisoformat(modified_logs[0].rsplit(' ', 1)[0])
                if modified_logs else None
            )

            return creation_date, last_modified

        except git.exc.GitError as e:
            self.logger.error(f"Failed to get Git history for {project_path}: {str(e)}")
            return None, None

    def _get_repo_web_url(self, project_name: str) -> str:
        """
        Get web URL for the project in repository
        
        Args:
            project_name: Name of the project
            
        Returns:
            Full web URL to project in repository
        """
        try:
            if not self.repo:
                self.logger.warning("Git repository not initialized")
                return ""

            # Get remote URL (usually ends with .git)
            remote_url = self.repo.remotes.origin.url
            
            # Convert SSH URL to HTTPS if needed
            if remote_url.startswith('git@'):
                # Convert git@gitlab.fast.bi:bi-platform/... to https://gitlab.fast.bi/bi-platform/...
                remote_url = remote_url.replace(':', '/').replace('git@', 'https://')
            
            # Remove .git suffix and auth token if present
            base_url = remote_url.replace('.git', '').split('@')[-1]
            if 'oauth2:' in base_url:
                base_url = base_url.split('oauth2:')[1].split('@')[-1]
                
            # Get current branch
            branch = self.repo.active_branch.name
            
            # Construct web URL
            return f"https://{base_url}/tree/{branch}/{project_name}?ref_type=heads"
            
        except Exception as e:
            self.logger.error(f"Failed to construct repository URL: {e}")
            return ""

    def get_project_info(self, project_name: str) -> Dict:
        """
        Get comprehensive project information including Git-based dates and proper URLs
        
        Args:
            project_name: Name of the project
            
        Returns:
            Dictionary containing project information or None if project not found
        """
        project_path = self.base_path / 'dbt_projects' / project_name
        
        if not project_path.exists():
            return None

        try:
            variables = self._read_variables_file(project_name)
            
            # Get Git-based dates
            relative_path = self._get_repo_web_url(project_name)  # Get full repo URL
            created_at, last_modified = self._get_git_dates(project_name)
            
            # If Git dates not available, fall back to filesystem dates
            if not created_at or not last_modified:
                self.logger.warning(f"Using filesystem dates for {project_name}")
                created_at = datetime.fromtimestamp(project_path.stat().st_ctime)
                last_modified = datetime.fromtimestamp(project_path.stat().st_mtime)
            
            # Basic project info
            info = {
                'project_name': project_name,
                'owner': variables.get('DAG_OWNER', 'Default'),
                'airbyte_id': variables.get('AIRBYTE_CONNECTION_ID'),
                'airbyte_workspace': variables.get('AIRBYTE_WORKSPACE_ID'),
                'airflow_dag_name': variables.get('DAG_ID', f'dbt_{project_name}'),
                'repo_path': str(relative_path),  # Using relative path from Git root
                'created_at': created_at,
                'last_modified': last_modified,
                'data_quality_enabled': variables.get('DATA_QUALITY', False),
                'dbt_docs_enabled': variables.get('DBT_DOCS_ENABLED', True),
                'dbt_project_level': variables.get('PROJECT_LEVEL'),
            }

            # Add URLs if enabled
            if variables.get('DATA_QUALITY', '').lower() == 'true' or variables.get('DATA_QUALITY', False) is True:
                info['data_quality_url'] = self._construct_url(project_name, 'quality')
            if variables.get('DBT_DOCS_ENABLED', '').lower() == 'true' or variables.get('DBT_DOCS_ENABLED', True) is True:
                info['dbt_docs_url'] = self._construct_url(project_name, 'docs')

            return info

        except Exception as e:
            self.logger.error(f"Error getting project info for {project_name}: {str(e)}")
            raise

    def get_creation_date(self, project_name: str) -> Optional[datetime]:
        """Get project creation date"""
        project_path = self.base_path / 'dbt_projects' / project_name
        if not project_path.exists():
            return None
        return datetime.fromtimestamp(project_path.stat().st_ctime)

    def get_last_modified_date(self, project_name: str) -> Optional[datetime]:
        """Get project last modified date"""
        project_path = self.base_path / 'dbt_projects' / project_name
        if not project_path.exists():
            return None
        return datetime.fromtimestamp(project_path.stat().st_mtime)

    def compile_dbt_manifest(self, project_name: str) -> Optional[Path]:
        """
        Compile DBT manifest file for a project if target folder doesn't exist
        
        Args:
            project_name: Name of the DBT project
            
        Returns:
            Path: Path to manifest.json if successful, None if failed 
        """
        project_path = self.base_path / 'dbt_projects' / project_name
        target_path = project_path / 'target'
        manifest_path = target_path / 'manifest.json'
        
        if not project_path.exists():
            self.logger.error(f"Project path {project_path} does not exist")
            return None
            
        # Return existing manifest if target folder exists
        if target_path.exists() and manifest_path.exists():
            self.logger.info(f"Using existing manifest for {project_name}")
            return manifest_path
            
        try:
            self.logger.info(f"Compiling new manifest for {project_name}")
            
            # Initialize warehouse auth manager with project path
            auth_manager = WarehouseAuthManager(self.logger, str(project_path))
            
            # Execute dbt commands within the auth context
            with auth_manager.get_auth_context():
                # Run dbt deps
                deps_result = subprocess.run(
                    ['dbt', 'deps', '--quiet'],
                    cwd=project_path,
                    check=True,
                    capture_output=True,
                    text=True
                )
                self.logger.debug(f"dbt deps output: {deps_result.stdout}")
                
                # Run dbt compile
                compile_result = subprocess.run(
                    ['dbt', 'compile', '--quiet'],
                    cwd=project_path,
                    check=True,
                    capture_output=True,
                    text=True
                )
                self.logger.debug(f"dbt compile output: {compile_result.stdout}")
            
            if manifest_path.exists():
                self.logger.info(f"Successfully compiled manifest for {project_name}")
                return manifest_path
                
            self.logger.error(f"Manifest file not generated at {manifest_path}")
            return None
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"DBT command failed for {project_name}: {e.stderr}")
            return None
        except Exception as e:
            self.logger.error(f"Error compiling manifest for {project_name}: {str(e)}")
            return None

    def clean_dbt_project(self, project_name: str) -> bool:
        """
        Clean DBT project target folder using 'dbt clean' command
        
        Args:
            project_name: Name of the DBT project
            
        Returns:
            bool: True if cleaning was successful, False otherwise
        """
        project_path = self.base_path / 'dbt_projects' / project_name
        target_path = project_path / 'target'
        
        if not project_path.exists():
            self.logger.error(f"Project path {project_path} does not exist")
            return False
            
        # Only run clean if target folder exists
        if not target_path.exists():
            self.logger.info(f"No target folder found for {project_name}, skipping clean")
            return True
                
        try:
            self.logger.info(f"Cleaning project {project_name}")
            result = subprocess.run(['dbt', 'clean', '--quiet'],
                            cwd=project_path,
                            check=True,
                            capture_output=True,
                            text=True)
            
            # Verify target folder was removed
            if not target_path.exists():
                self.logger.info(f"Successfully cleaned project {project_name}")
                return True
                
            self.logger.error(f"Target folder still exists after clean for {project_name}")
            return False
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"DBT clean command failed for {project_name}: {e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"Error cleaning project {project_name}: {str(e)}")
            return False

    @staticmethod
    def count_items(file_path, item_type):
        # Define valid item types and their keys in the JSON structure
        valid_types = {
            "models": "nodes",
            "seeds": "nodes", 
            "tests": "nodes",
            "snapshots": "nodes",
            "sources": "sources"
        }
        
        if item_type not in valid_types:
            raise ValueError("Invalid item type. Choose from 'models', 'seeds', 'snapshots', or 'sources'.")

        # Read manifest.json
        with open(file_path, 'r') as file:
            data = json.load(file)
            
        # Filter items by 'resource_type' and count items
        resource_type = item_type[:-1] # delete last symbol models -> model to get resource_type      
        count_val = sum(1 for item in data.get(valid_types[item_type], {}).values() 
                    if item.get('resource_type') == resource_type and item.get('package_name')!= 're_data')

        return count_val

class WarehouseAuthManager:
    """Handles authentication for different data warehouse types"""
    
    def __init__(self, logger, project_path: str = None):
        self.logger = logger
        self.temp_dirs = []
        # Check if we should use new authentication methods
        self.use_new_auth = os.getenv('DATA_WAREHOUSE_SECRET', '0') == '1'
        
        # If using new auth and project path is provided, extract warehouse type
        self.warehouse_type = None
        if self.use_new_auth and project_path:
            self.warehouse_type = self._extract_warehouse_type(project_path)
            self.logger.info(f"Detected warehouse type: {self.warehouse_type}")

    def _extract_warehouse_type(self, project_path: str) -> str:
        """Extract warehouse type from project's profiles.yml"""
        try:
            profiles_path = Path(project_path) / 'profiles.yml'
            if not profiles_path.exists():
                raise ValueError(f"profiles.yml not found in {project_path}")
            
            with open(profiles_path, 'r') as f:
                profiles = yaml.safe_load(f)
                
            # Get the first profile (usually the only one)
            first_profile = next(iter(profiles.values()))
            target = first_profile.get('target')
            outputs = first_profile.get('outputs', {})
            target_config = outputs.get(target, {})
            
            warehouse_type = target_config.get('type')
            if not warehouse_type:
                raise ValueError(f"No warehouse type found in profiles.yml for target {target}")
            
            return warehouse_type.lower()
            
        except Exception as e:
            self.logger.error(f"Failed to extract warehouse type: {str(e)}")
            raise

    def _read_secret(self, secret_name: str) -> str:
        """Read a secret from the mounted volume"""
        secret_path = Path(f"/fastbi/secrets/{self.warehouse_type}/{secret_name}")
        if not secret_path.exists():
            raise ValueError(f"Secret {secret_name} not found at {secret_path}")
        
        with open(secret_path, 'r') as f:
            return f.read().strip()

    def cleanup(self):
        """Clean up any temporary directories and files"""
        for temp_dir in self.temp_dirs:
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                self.logger.warning(f"Failed to cleanup temp directory {temp_dir}: {str(e)}")
        self.temp_dirs = []

    @contextmanager
    def bigquery_auth_legacy(self):
        """Context manager for BigQuery authentication (legacy method)"""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        creds_path = Path(temp_dir) / "sa.json"
        
        try:
            # Get and decode service account JSON from environment variable
            sa_secret = os.getenv('GCP_SA_SECRET')
            if not sa_secret:
                raise ValueError("GCP_SA_SECRET environment variable not set")
            
            sa_json = base64.b64decode(sa_secret).decode('utf-8')
            
            # Write service account JSON to temporary file
            with open(creds_path, 'w') as f:
                f.write(sa_json)
            
            # Set environment variable
            old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(creds_path)
            
            # Activate service account
            subprocess.run(
                ['gcloud', 'auth', 'activate-service-account', '--key-file', str(creds_path)],
                check=True,
                capture_output=True
            )
            
            yield
            
        finally:
            # Revoke credentials and restore original state
            try:
                subprocess.run(['gcloud', 'auth', 'revoke'], check=True, capture_output=True)
            except subprocess.CalledProcessError:
                self.logger.warning("Failed to revoke GCloud credentials")
                
            if old_creds:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = old_creds
            else:
                os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
            
            self.cleanup()

    @contextmanager
    def bigquery_auth(self):
        """Context manager for BigQuery authentication"""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        creds_path = Path(temp_dir) / "sa.json"
        
        try:
            # Read service account secret
            sa_secret = self._read_secret('DBT_DEPLOY_GCP_SA_SECRET')
            
            # Decode and write service account JSON
            sa_json = base64.b64decode(sa_secret).decode('utf-8')
            with open(creds_path, 'w') as f:
                f.write(sa_json)
            
            # Set environment variable
            old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(creds_path)
            
            # Activate service account
            subprocess.run(
                ['gcloud', 'auth', 'activate-service-account', '--key-file', str(creds_path)],
                check=True,
                capture_output=True
            )
            
            # Set additional BigQuery environment variables
            for env_var in ['BIGQUERY_PROJECT_ID', 'BIGQUERY_REGION', 'DATA_ANALYSIS_GCP_SA_EMAIL']:
                try:
                    os.environ[env_var] = self._read_secret(env_var)
                    self.logger.info(f"{env_var} is set")
                except Exception as e:
                    self.logger.warning(f"Failed to set {env_var}: {str(e)}")
            
            yield
            
        finally:
            # Revoke credentials and restore original state
            try:
                subprocess.run(['gcloud', 'auth', 'revoke'], check=True, capture_output=True)
            except subprocess.CalledProcessError:
                self.logger.warning("Failed to revoke GCloud credentials")
                
            if old_creds:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = old_creds
            else:
                os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
            
            self.cleanup()

    @contextmanager
    def snowflake_auth(self):
        """Context manager for Snowflake authentication"""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        
        try:
            # Create snowsql secrets directory
            snowsql_dir = Path("/snowsql/secrets/")
            snowsql_dir.mkdir(parents=True, exist_ok=True)
            
            # Read private key and write to snowsql directory
            private_key = self._read_secret('SNOWFLAKE_PRIVATE_KEY')
            with open(snowsql_dir / "rsa_key.p8", 'w') as f:
                f.write(private_key)
            
            # Set permissions
            os.chmod(snowsql_dir / "rsa_key.p8", 0o600)
            
            # Read and set passphrase
            os.environ['SNOWSQL_PRIVATE_KEY_PASSPHRASE'] = self._read_secret('SNOWFLAKE_PASSPHRASE')
            
            # Set additional Snowflake environment variables
            for env_var in ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_USER', 
                          'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_PASSWORD']:
                try:
                    os.environ[env_var] = self._read_secret(env_var)
                    self.logger.info(f"{env_var} is set")
                except Exception as e:
                    self.logger.warning(f"Failed to set {env_var}: {str(e)}")
            
            self.logger.info("Snowflake secrets configured")
            yield
            
        finally:
            self.cleanup()

    @contextmanager
    def redshift_auth(self):
        """Context manager for Redshift authentication"""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        
        try:
            # Set Redshift environment variables
            for env_var in ['REDSHIFT_PASSWORD', 'REDSHIFT_USER', 'REDSHIFT_HOST', 
                          'REDSHIFT_PORT', 'REDSHIFT_DATABASE']:
                try:
                    os.environ[env_var] = self._read_secret(env_var)
                    self.logger.info(f"{env_var} is set")
                except Exception as e:
                    self.logger.warning(f"Failed to set {env_var}: {str(e)}")
            
            self.logger.info("Redshift secrets configured")
            yield
            
        finally:
            self.cleanup()

    @contextmanager
    def fabric_auth(self):
        """Context manager for Fabric authentication"""
        temp_dir = tempfile.mkdtemp()
        self.temp_dirs.append(temp_dir)
        
        try:
            # Set Fabric environment variables
            for env_var in ['FABRIC_USER', 'FABRIC_PASSWORD', 'FABRIC_SERVER', 
                          'FABRIC_DATABASE', 'FABRIC_PORT', 'FABRIC_AUTHENTICATION']:
                try:
                    os.environ[env_var] = self._read_secret(env_var)
                    self.logger.info(f"{env_var} is set")
                except Exception as e:
                    self.logger.warning(f"Failed to set {env_var}: {str(e)}")
            
            self.logger.info("Fabric secrets configured")
            yield
            
        finally:
            self.cleanup()

    @contextmanager
    def get_auth_context(self):
        """Get the appropriate authentication context based on warehouse type"""
        if self.use_new_auth and self.warehouse_type:
            # Use new authentication method based on warehouse type
            auth_handlers = {
                'bigquery': self.bigquery_auth,
                'snowflake': self.snowflake_auth,
                'redshift': self.redshift_auth,
                'fabric': self.fabric_auth
            }
            
            handler = auth_handlers.get(self.warehouse_type)
            if not handler:
                raise ValueError(f"Unsupported warehouse type: {self.warehouse_type}")
            
            with handler() as context:
                yield context
        else:
            # Fallback to old BigQuery authentication for backward compatibility
            with self.bigquery_auth_legacy() as context:
                yield context

class ProjectDependencyManager:
    """Handles deletion of project dependencies (Airflow, Data Quality, Data Catalog)"""
    
    def __init__(self, base_path: str = '/tmp/dbt_management'):
        # Initialize configuration from environment variables
        self.airflow_config = {
            'base_url': Config.DATA_ORCHESTRATOR_BASE_URL,
            'user': Config.DATA_ORCHESTRATOR_BASE_USER,
            'password': Config.DATA_ORCHESTRATOR_BASE_USER_PASSWORD,
            'dags_repo': Config.DATA_ORCHESTRATOR_REPO_URL
        }
        self.data_catalog_url = Config.DC_DQ_ENDPOINT_URL
        self.data_quality_url = Config.DC_DQ_ENDPOINT_URL
        self.base_path = base_path
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
    def _get_airflow_auth(self) -> Tuple[str, str]:
        """Returns Airflow authentication credentials"""
        return (self.airflow_config['user'], self.airflow_config['password'])

    def delete_airflow_dag(self, project_name: str, metadata_manager: Optional[ProjectMetadataManager] = None) -> bool:
        """
        Delete Airflow DAG and its files
        
        Args:
            project_name: Name of the project
            metadata_manager: Instance of ProjectMetadataManager to access project variables
            
        Returns:
            bool: True if deletion was successful
        """
        try:
            if metadata_manager is None:
                # Create a new metadata manager if one wasn't provided
                metadata_manager = ProjectMetadataManager(base_path=self.base_path)

            # Get DAG info directly from variables file
            variables = metadata_manager._read_variables_file(project_name)
            dag_name = variables.get('DAG_ID', f'dbt_{project_name}')
            self.domain = variables.get('DOMAIN', Config.DOMAIN)
            
            if not dag_name:
                self.logger.warning(f"No DAG name found for project {project_name}")
                return False

            # Check if DAG exists in Airflow
            check_url = f"{self.airflow_config['base_url']}/api/v1/dags/{dag_name}"
            response = requests.get(
                check_url,
                auth=self._get_airflow_auth()
            )
            
            # If DAG exists in Airflow, delete it
            if response.status_code == 200:
                delete_response = requests.delete(
                    check_url,
                    auth=self._get_airflow_auth()
                )
                
                if delete_response.status_code not in [200, 204]:
                    self.logger.error(f"Failed to delete DAG {dag_name} from Airflow. Status: {delete_response.status_code}")
                    return False
                    
                self.logger.info(f"Successfully deleted DAG {dag_name} from Airflow")
            elif response.status_code != 404:
                # If response is not 404 (not found), then there was an error checking DAG existence
                self.logger.error(f"Error checking DAG existence. Status: {response.status_code}")
                return False

            # Now handle the DAGs repository
            dags_repo_url = self.airflow_config['dags_repo']
            dags_repo_token = Config.GROUP_ACCESS_TOKEN or os.getenv('GROUP_ACCESS_TOKEN')

            # Create a unique temporary directory for DAGs repo
            with tempfile.TemporaryDirectory(prefix='dags_repo_') as temp_dags_dir:
                try:
                    # Add credentials to repo URL
                    dags_repo_url_with_token = dags_repo_url.replace(
                        "https://", f"https://oauth2:{dags_repo_token}@"
                    )
                    
                    # Clone the DAGs repo
                    dags_repo = git.Repo.clone_from(
                        dags_repo_url_with_token,
                        temp_dags_dir
                    )
                    
                    # Delete the project folder
                    dag_path = Path(temp_dags_dir) / 'dags' / project_name
                    if dag_path.exists():
                        self.logger.info(f"Deleting DAG folder for project {project_name}")
                        shutil.rmtree(dag_path)
                        
                        # Verify folder was actually deleted
                        if dag_path.exists():
                            self.logger.error(f"Failed to delete DAG folder for project {project_name}")
                            return False
                            
                        try:
                            # Configure git user for the commit
                            with dags_repo.config_writer() as git_config:
                                git_config.set_value('user', 'name', 'Fast.BI Bot')
                                git_config.set_value('user', 'email', f'bot@{self.domain}')
                            
                            # Get default branch
                            try:
                                default_branch = dags_repo.git.rev_parse("--abbrev-ref", "origin/HEAD").split('/')[-1]
                            except git.GitCommandError:
                                default_branch = "master"  # Fallback to master if can't detect
                            
                            # Commit and push changes
                            dags_repo.git.add(A=True)
                            dags_repo.index.commit(f"Deleted DAG files for project {project_name}")
                            
                            # Push with proper authentication
                            with dags_repo.git.custom_environment(GIT_SSH_COMMAND=f'git push origin {default_branch}'):
                                dags_repo.remotes.origin.push()
                            
                            self.logger.info(f"Successfully pushed DAG deletion for project {project_name}")
                            return True
                            
                        except git.GitCommandError as git_err:
                            self.logger.error(f"Git operation failed for DAGs repo: {git_err}")
                            return False
                    else:
                        self.logger.info(f"DAG folder for project {project_name} does not exist in repository")
                        return True  # Consider it a success if folder doesn't exist
                        
                except Exception as e:
                    self.logger.error(f"Failed to handle DAGs repository: {str(e)}")
                    return False
            
            return True  # If we got here, everything succeeded
            
        except Exception as e:
            self.logger.error(f"Error deleting Airflow DAG: {str(e)}")
            return False

    def _get_dcdq_auth_headers(self) -> Dict[str, str]:
        """Returns Data Catalog/Quality Bearer token authentication headers"""
        token = Config.DC_DQ_BEARER_TOKEN
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def _transform_project_name(self, project_name: str) -> str:
        """
        Transform project name to DCDQ metadata format
        Example: gk_test_prj_mgmt_dbt_elt -> gk-test-prj-mgmt-dbt-elt
        
        Args:
            project_name: Original project name
            
        Returns:
            str: Transformed project name for DCDQ
        """
        return project_name.replace('_', '-').lower()

    def delete_data_catalog(self, project_name: str) -> bool:
        """
        Delete project from Data Catalog
        
        Args:
            project_name: Name of the project
            
        Returns:
            bool: True if deletion was successful
        """
        try:
            # Transform project name for DCDQ
            dcdq_project_name = self._transform_project_name(project_name)
            
            # Get auth headers
            headers = self._get_dcdq_auth_headers()
            
            # First get the catalog ID
            response = requests.get(
                f"{self.data_catalog_url}/data-catalog/{dcdq_project_name}",
                headers=headers
            )
            
            if response.status_code in [200, 201]:  # Accept both 200 and 201
                try:
                    catalog_data = response.json()
                    catalog_id = catalog_data.get('id')
                    
                    if catalog_id:
                        self.logger.info(f"Found Data Catalog entry with ID {catalog_id}")
                        # Delete the catalog entry
                        delete_response = requests.delete(
                            f"{self.data_catalog_url}/data-catalog/{catalog_id}",
                            headers=headers
                        )
                        
                        if delete_response.status_code not in [200, 204]:
                            self.logger.error(
                                f"Failed to delete Data Catalog entry {catalog_id}. "
                                f"Status: {delete_response.status_code}"
                            )
                            return False
                        
                        self.logger.info(f"Successfully deleted Data Catalog entry {catalog_id}")
                        return True
                    else:
                        self.logger.warning(f"No ID found in response for {dcdq_project_name}")
                        return False
                except ValueError as e:
                    self.logger.error(f"Error parsing response JSON: {str(e)}")
                    return False
                    
            elif response.status_code == 404:
                self.logger.info(f"No Data Catalog entry found for {dcdq_project_name}")
                return True  # Consider it a success if entry doesn't exist
            else:
                self.logger.error(
                    f"Error checking Data Catalog entry existence. "
                    f"Status: {response.status_code}, Response: {response.text}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Error deleting Data Catalog entry: {str(e)}")
            return False

    def delete_data_quality(self, project_name: str) -> bool:
        """
        Delete project from Data Quality
        
        Args:
            project_name: Name of the project
            
        Returns:
            bool: True if deletion was successful
        """
        try:
            # Transform project name for DCDQ
            dcdq_project_name = self._transform_project_name(project_name)
            
            # Get auth headers
            headers = self._get_dcdq_auth_headers()
            
            # Check for data quality entry
            response = requests.get(
                f"{self.data_quality_url}/data-quality/{dcdq_project_name}",
                headers=headers
            )
            
            if response.status_code in [200, 201]:  # Accept both 200 and 201
                try:
                    quality_data = response.json()
                    quality_id = quality_data.get('id')
                    
                    if quality_id:
                        self.logger.info(f"Found Data Quality entry with ID {quality_id}")
                        # Delete the quality entry
                        delete_response = requests.delete(
                            f"{self.data_quality_url}/data-quality/{quality_id}",
                            headers=headers
                        )
                        
                        if delete_response.status_code not in [200, 204]:
                            self.logger.error(
                                f"Failed to delete Data Quality entry {quality_id}. "
                                f"Status: {delete_response.status_code}"
                            )
                            return False
                        
                        self.logger.info(f"Successfully deleted Data Quality entry {quality_id}")
                        return True
                    else:
                        self.logger.warning(f"No ID found in response for {dcdq_project_name}")
                        return False
                except ValueError as e:
                    self.logger.error(f"Error parsing response JSON: {str(e)}")
                    return False
                    
            elif response.status_code == 404:
                self.logger.info(f"No Data Quality entry found for {dcdq_project_name}")
                return True  # Consider it a success if entry doesn't exist
            else:
                self.logger.error(
                    f"Error checking Data Quality entry existence. "
                    f"Status: {response.status_code}, Response: {response.text}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"Error deleting Data Quality entry: {str(e)}")
            return False

def setup_routes(app: APIBlueprint):
    project_manager = ProjectManager(
        repo_url=Config.DATA_MODEL_REPO_URL or os.getenv('DATA_MODEL_REPO_URL'),
        repo_token=Config.GROUP_ACCESS_TOKEN or os.getenv('GROUP_ACCESS_TOKEN'),
        base_path='/tmp/dbt_management'
    )
    # Pass repo from ProjectManager to ProjectMetadataManager
    metadata_manager = ProjectMetadataManager(
        base_path='/tmp/dbt_management',
        repo=project_manager.repo
    )

    # Common date retrieval function
    def get_project_dates_from_list(project_name: str, projects: List[Dict]) -> Optional[Dict]:
        return next(
            (
                {
                    'created_at': p['created_at'],
                    'last_modified': p['last_modified']
                }
                for p in projects if p['project_name'] == project_name
            ),
            None
        )

#POST routes:

    @app.post('/repository/refresh')
    @app.doc(
        tags=['Project Management-Repository'],
        summary='Refresh Repository',
        description='Forces a refresh of the Git repository by pulling latest changes.'
    )
    @app.auth_required(auth)
    @app.output(RefreshResponseSchema)
    def refresh_repository():
        """
        Force refresh the git repository by pulling latest changes
        
        This endpoint should be called before operations that require
        up-to-date repository state.
        """
        result = project_manager.refresh_repository()
        
        if not result["success"]:
            abort(500, message=result["message"])
            
        return {
            "success": result["success"],
            "message": result["message"],
            "timestamp": datetime.now()
        }

    @app.post('/projects/<project_name>/archive')
    @app.doc(
        tags=['Project Management-Project Operations'],
        summary='Archive Project',
        description='Creates a zip archive of the project and uploads it to MinIO storage.'
    )
    @app.auth_required(auth)
    def archive_project(project_name):
        """Archive a DBT project to MinIO storage"""
        try:
            minio_key = project_manager.archive_project(project_name)
            minio_endpoint = Config.MINIO_ENDPOINT or os.getenv('MINIO_ENDPOINT')
            bucket_name = Config.MINIO_BUCKET_NAME or os.getenv('MINIO_BUCKET_NAME', 'dbt-project-archive')
            
            return {
                'message': f"Project {project_name} archived successfully",
                'minio_key': minio_key,
                'minio_url': f"https://{minio_endpoint}/{bucket_name}/{minio_key}"
            }
        except FileNotFoundError:
            abort(404, message=f"Project {project_name} not found")
        except S3Error as e:
            abort(500, message=f"MinIO storage error: {str(e)}")
        except Exception as e:
            abort(500, message=f"Failed to archive project: {str(e)}")

    @app.post('/projects/<project_name>/variables')
    @app.doc(
        tags=['Project Management-Variables'],
        summary='Update Project Variables',
        description='Updates project variables and creates a new branch with the changes.'
    )
    @app.auth_required(auth)
    @app.input(UpdateVariablesInputSchema)
    def update_project_variables(project_name, json_data):
        """Update project variables"""
        success = project_manager.update_project_variables(
            project_name,
            json_data['variables'],
            json_data['branch_name']
        )
        
        if not success:
            abort(404, message=f"Project {project_name} not found")
        
        return {
            'message': f"Variables updated successfully for project {project_name}",
            'branch_name': json_data['branch_name']
        }
    
    @app.post('/projects/<project_name>/profiles')
    @app.doc(
        tags=['Project Management-Profile'],
        summary='Update Project Profile',
        description='Updates project profile.yml and creates a new branch with the changes.'
    )
    @app.auth_required(auth)
    @app.input(UpdateProfilesInputSchema)
    def update_project_profiles(project_name, json_data):
        """Update project variables"""
        success = project_manager.update_project_profiles(
            project_name,
            json_data['profiles'],
            json_data['branch_name']
        )
        
        if not success:
            abort(404, message=f"Project {project_name} not found")
        
        return {
            'message': f"Profiles updated successfully for project {project_name}",
            'branch_name': json_data['branch_name']
        }

    @app.post('/projects/<project_name>/refresh')
    @app.doc(
        tags=['Project Management-Project Operations'],
        summary='Refresh Project Schema',
        description='Refreshes source schema for the DBT project.'
    )
    @app.auth_required(auth)
    @app.input(RefreshInputSchema)
    def refresh_project(project_name, json_data):
        """Refresh source schema for a DBT project"""
        # This is a placeholder for the backfill implementation
        # You would implement the actual backfill logic here
        return {
            'message': f"Refresh initiated for project {project_name}"
        }
    
    @app.post('/projects/<project_name>/rename')
    @app.doc(
        tags=['Project Management-Project Operations'],
        summary='Rename Project',
        description='Renames the DBT project, updates all dependencies, and cleans up old project dependencies.'
    )
    @app.auth_required(auth)
    @app.input(RenameInputSchema)
    def rename_project(project_name, json_data):
        """
        Rename the DBT project and all its dependencies
        
        This endpoint:
        1. Validates new project name
        2. Updates all project files with the new name
        3. Creates a new branch with all changes
        4. Cleans up old project dependencies (Airflow DAG, Data Quality, Data Catalog)
        """
        try:
            # Validate project exists
            project_path = Path(project_manager.base_path) / 'dbt_projects' / project_name
            if not project_path.exists():
                abort(404, message=f"Project {project_name} not found")


            # Call ProjectManager method to rename project
            result = project_manager.rename_project(
                old_project_name=project_name,
                new_project_name=json_data['new_project_name']
            )

            if not result['success']:
                abort(500, message=result['message'])

            # Clean up old project dependencies
            success, deletion_status = project_manager.delete_project(
                project_name=project_name,
                delete_data=False,  # Don't delete warehouse data
                delete_folder=True  # Delete folder and dependencies
            )

            if not success:
                # Log warning but don't fail the rename operation
                print(
                    f"Failed to clean up old project dependencies for {project_name}. "
                    f"Manual cleanup may be required. Status: {deletion_status}"
                )
                
                # Add warning to response
                result['message'] += " (Warning: Failed to clean up some old dependencies)"
            else:
                # Add cleanup status to response message
                cleanup_parts = []
                if deletion_status.get('airflow_deleted'):
                    cleanup_parts.append("Airflow DAG")
                if deletion_status.get('data_catalog_deleted'):
                    cleanup_parts.append("Data Catalog entry")
                if deletion_status.get('data_quality_deleted'):
                    cleanup_parts.append("Data Quality entry")
                
                if cleanup_parts:
                    result['message'] += f" (Cleaned up: {', '.join(cleanup_parts)})"

            # Prepare response
            response = {
                'message': result['message'],
                'old_project_name': result['old_project_name'],
                'new_project_name': result['new_project_name'],
                'branch_name': result['branch_name']
            }

            # Add branch URL if available
            if result.get('branch_url'):
                response['branch_url'] = result['branch_url']

            # Add cleanup status to response
            response['cleanup_status'] = deletion_status

            return response

        except Exception as e:
            abort(500, message=f"Failed to rename project: {str(e)}")


    # Update the existing route implementation
    @app.post('/projects/<project_name>/owner')
    @app.doc(
        tags=['Project Management-Project Operations'],
        summary='Update Project Owner',
        description='Updates the DBT project owner and creates a new branch with the changes.'
    )
    @app.auth_required(auth)
    @app.input(OwnerInputSchema)
    def update_project_owner(project_name, json_data):
        """
        Update the DBT project owner
        
        This endpoint:
        1. Updates the DAG_OWNER in the project's variables file
        2. Creates a new branch with the changes
        3. Returns branch information for manual merge request creation
        """
        try:
            # Validate project exists
            project_path = Path(project_manager.base_path) / 'dbt_projects' / project_name
            if not project_path.exists():
                abort(404, message=f"Project {project_name} not found")

            # Call ProjectManager method to update owner
            result = project_manager.update_project_owner(
                project_name=project_name,
                new_owner=json_data['owner_name']
            )

            if not result['success']:
                abort(500, message=result['message'])

            response = {
                'success': True,
                'message': result['message'],
                'project_name': result['project_name'],
                'new_owner': result['new_owner'],
                'branch_name': result['branch_name']
            }

            # Add branch URL if available
            if result.get('branch_url'):
                response['branch_url'] = result['branch_url']

            return response

        except Exception as e:
            abort(500, message=f"Failed to update project owner: {str(e)}")

#GET routes:

    @app.get('/projects')
    @app.doc(
        tags=['Project Management-Projects'],
        summary='List DBT Projects',
        description='Lists all DBT projects in the repository with their Git-based creation and modification dates.'
    )
    @app.auth_required(auth)
    @app.output(ProjectListSchema(many=True))
    def list_projects():
        """
        List all DBT projects in the repository
        
        Returns a list of projects with their Git-based creation and modification dates.
        """
        try:
            # First refresh the repository
            refresh_result = project_manager.refresh_repository()
            if not refresh_result["success"]:
                abort(500, message=f"Failed to refresh repository: {refresh_result['message']}")
            
            # Get projects with Git-based dates
            projects = project_manager.list_projects()
            return projects
            
        except Exception as e:
            abort(500, message=f"Failed to list projects: {str(e)}")

    
    @app.get('/projects/<project_name>/variables')
    @app.doc(
        tags=['Project Management-Variables'],
        summary='Get Project Variables',
        description='Retrieves variables from dbt_airflow_variables.yml for the specified project.'
    )
    @app.auth_required(auth)
    @app.output(ProjectVariablesSchema)
    def get_project_variables(project_name):
        """Get project variables"""
        variables = project_manager.get_project_variables(project_name)
        if not variables:
            abort(404, message=f"Variables not found for project {project_name}")
        return {'project_name': project_name, 'variables': variables}
    
    @app.get('/projects/<project_name>/profiles')
    @app.doc(
        tags=['Project Management-Profile'],
        summary='Get Project Profiles',
        description='Retrieves profile configuration from profiles.yml for the specified project.'
    )
    @app.auth_required(auth)
    @app.output(ProjectVariablesSchema)
    def get_project_profiles(project_name):
        """Get project profiles"""
        profiles = project_manager.get_project_profiles(project_name)
        if not profiles:
            abort(404, message=f"Profiles not found for project {project_name}")
        return {'project_name': project_name, 'variables': profiles}

    @app.get('/projects/<project_name>/info')
    @app.doc(
        tags=['Project Management-Project Info'],
        summary='Get Project Information',
        description='Retrieves comprehensive information about a DBT project including Git history, configuration, and enabled features.'
    )
    @app.auth_required(auth)
    @app.output(ProjectInfoSchema)
    def get_project_info(project_name):
        """Get comprehensive project information"""
        try:
            # Get project dates from list_projects (single source of truth)
            projects = project_manager.list_projects()
            dates = get_project_dates_from_list(project_name, projects)
            
            if not dates:
                abort(404, message=f"Project {project_name} not found")

            # Get project info
            info = metadata_manager.get_project_info(project_name)
            if not info:
                abort(404, message=f"Project {project_name} not found")

            # Update dates with Git-based dates
            info['created_at'] = dates['created_at']
            info['last_modified'] = dates['last_modified']

            return info
        except Exception as e:
            abort(500, message=f"Failed to get project info: {str(e)}")

    @app.get('/projects/<project_name>/owner')
    @app.doc(
        tags=['Project Management-Project Info'],
        summary='Get Project Owner',
        description='Retrieves the owner information for a specific DBT project.'
    )
    @app.auth_required(auth)
    def get_project_owner(project_name):
        """Get project owner"""
        variables = metadata_manager._read_variables_file(project_name)
        owner = variables.get('DAG_OWNER', 'Default')
        return {'owner': owner}

    @app.get('/projects/<project_name>/airbyte')
    @app.doc(
        tags=['Project Management-Integrations'],
        summary='Get Airbyte Configuration',
        description='Retrieves Airbyte connection information and status for the specified project.'
    )
    @app.auth_required(auth)
    def get_airbyte_info(project_name):
        """
        Get Airbyte connection information
        
        Returns:
            dict: Dictionary containing Airbyte connection ID and enabled status
        """
        variables = metadata_manager._read_variables_file(project_name)
        airbyte_connection_id = variables.get('AIRBYTE_CONNECTION_ID')
        init_version = get_airbyte_destination_version(airbyte_connection_id)

        return {
            'connection_id': airbyte_connection_id,
            'enabled': variables.get('AIRBYTE_REPLICATION_FLAG', '').lower() == 'true',
            'init_version': init_version
        }

    @app.get('/projects/<project_name>/airflow')
    @app.doc(
        tags=['Project Management-Integrations'],
        summary='Get Airflow Configuration',
        description='Retrieves Airflow DAG configuration and scheduling information.'
    )
    @app.auth_required(auth)
    def get_airflow_info(project_name):
        """Get Airflow DAG information"""
        variables = metadata_manager._read_variables_file(project_name)
        return {
            'dag_name': variables.get('DAG_ID', f'dbt_{project_name}'),
            'schedule_interval': variables.get('DAG_SCHEDULE_INTERVAL'),
            'dag_tags': variables.get('DAG_TAG', False)
        }

    @app.get('/projects/<project_name>/dates')
    @app.doc(
        tags=['Project Management-Project Info'],
        summary='Get Project Dates',
        description='Retrieves creation and last modification dates from Git history.'
    )
    @app.auth_required(auth)
    @app.output(ProjectDatesSchema)
    def get_project_dates(project_name):
        """Get project creation and modification dates"""
        try:
            # Reuse the list_projects data
            projects = project_manager.list_projects()
            dates = get_project_dates_from_list(project_name, projects)
            
            if not dates:
                abort(404, message=f"Project {project_name} not found")
                
            return dates
            
        except Exception as e:
            abort(500, message=f"Failed to get project dates: {str(e)}")

    @app.get('/projects/<project_name>/status')
    @app.doc(
        tags=['Project Management-Project Info'],
        summary='Get Project Status',
        description='Retrieves status of enabled services and features for the project.'
    )
    @app.auth_required(auth)
    def get_project_status(project_name):
        """Get project status information of enabled services"""
        variables = metadata_manager._read_variables_file(project_name)
        
        status = {
            'data_quality_enabled': variables.get('DATA_QUALITY', False),
            'dbt_governance_enabled': variables.get('DATAHUB_ENABLED', False),
            'dbt_airbyte_replication': variables.get('AIRBYTE_REPLICATION_FLAG', False),
            'dbt_snapshot_sharding': variables.get('DBT_SNAPSHOT_SHARDING', False),
            'dbt_snapshot': variables.get('DBT_SNAPSHOT', False),
            'dbt_source_sharding': variables.get('DBT_SOURCE_SHARDING', False),
            'dbt_source': variables.get('DBT_SOURCE', False),
            'dbt_seed_sharding': variables.get('DBT_SEED_SHARDING', False),
            'dbt_seed': variables.get('DBT_SEED', False),
            'debug': variables.get('DEBUG', False)
        }

        return status

    @app.get('/projects/<project_name>/models')
    @app.doc(
       tags=['Project Management-Analytics'],
       summary='Get dbt Project Model Count',
       description='Retrieves the number of DBT models in the project.'
    )
    @app.auth_required(auth)
    def get_model_count(project_name):
       """Get number of DBT Project models"""
       manifest_path = metadata_manager.compile_dbt_manifest(project_name)
       if not manifest_path:
           abort(500, message="Failed to compile manifest file")
       
       try:
           count = metadata_manager.count_items(str(manifest_path), 'models')
           return {'project_name': project_name, 'models_count': count}
       except Exception as e:
           abort(500, message=str(e))
    
    @app.get('/projects/<project_name>/sources')
    @app.doc(
       tags=['Project Management-Analytics'],
       summary='Get dbt Project Sources Count', 
       description='Retrieves the number of DBT Sources in the project.'
    )
    @app.auth_required(auth)
    def get_sources_count(project_name):
       manifest_path = metadata_manager.compile_dbt_manifest(project_name)
       if not manifest_path:
           abort(500, message="Failed to compile manifest file")
    
       try:
           count = metadata_manager.count_items(str(manifest_path), 'sources')
           return {'project_name': project_name, 'sources_count': count}
       except Exception as e:
           abort(500, message=str(e))
    
    @app.get('/projects/<project_name>/seeds')
    @app.doc(
       tags=['Project Management-Analytics'],
       summary='Get dbt Project Seeds Count',
       description='Retrieves the number of DBT Seeds files in the project.'
    )
    @app.auth_required(auth)
    def get_seeds_count(project_name):
       manifest_path = metadata_manager.compile_dbt_manifest(project_name)
       if not manifest_path:
           abort(500, message="Failed to compile manifest file")
    
       try:
           count = metadata_manager.count_items(str(manifest_path), 'seeds')
           return {'project_name': project_name, 'seeds_count': count} 
       except Exception as e:
           abort(500, message=str(e))
    
    @app.get('/projects/<project_name>/snapshots')
    @app.doc(
       tags=['Project Management-Analytics'],
       summary='Get dbt Project Snapshots Count',
       description='Retrieves the number of DBT Snapshots in the project.'
    )
    @app.auth_required(auth)
    def get_snapshots_count(project_name):
       manifest_path = metadata_manager.compile_dbt_manifest(project_name)
       if not manifest_path:
           abort(500, message="Failed to compile manifest file")
    
       try:
           count = metadata_manager.count_items(str(manifest_path), 'snapshots')
           return {'project_name': project_name, 'snapshots_count': count}
       except Exception as e:
           abort(500, message=str(e))
    
    @app.get('/projects/<project_name>/tests')
    @app.doc(
       tags=['Project Management-Analytics'],
       summary='Get dbt Project Tests Count',
       description='Retrieves the number of DBT Tests in the project.'
    )
    @app.auth_required(auth)
    def get_tests_count(project_name):
       manifest_path = metadata_manager.compile_dbt_manifest(project_name)
       if not manifest_path:
           abort(500, message="Failed to compile manifest file")
    
       try:
           count = metadata_manager.count_items(str(manifest_path), 'tests')
           return {'project_name': project_name, 'tests_count': count}
       except Exception as e:
           abort(500, message=str(e))
    
    @app.get('/projects/<project_name>/tasks')
    @app.doc(
       tags=['Project Management-Analytics'],
       summary='Get dbt Project Tasks Count',
       description='Retrieves the count of dbt Project tasks (sum of Sources+Seeds+Models+Tests+Snapshots).'
    )
    @app.auth_required(auth)
    def get_airflow_task_count(project_name):
       manifest_path = metadata_manager.compile_dbt_manifest(project_name)
       if not manifest_path:
           abort(500, message="Failed to compile manifest file")
    
       try:
           tasks = {}
           total = 0
           for item_type in ['models', 'sources', 'seeds', 'snapshots', 'tests']:
               count = metadata_manager.count_items(str(manifest_path), item_type)
               tasks[f"{item_type}_count"] = count
               total += count
           
           return {
               'project_name': project_name,
               'tasks': tasks,
               'total_tasks': total
           }
       except Exception as e:
           abort(500, message=str(e))

    @app.get('/projects/<project_name>/packages')
    @app.doc(
        tags=['Project Management-Dependencies'],
        summary='Get DBT Packages',
        description='Lists installed DBT packages and their versions from packages.yml.'
    )
    @app.auth_required(auth)
    @app.output(PackageSchema(many=True))
    def get_dbt_packages(project_name):
        """Get list of installed DBT packages and their versions"""
        try:
            packages = metadata_manager._read_packages_file(project_name)
            if not packages:
                return []
            
            return packages
            
        except Exception as e:
            abort(500, message=f"Failed to read packages: {str(e)}")

#DELETE routes:

    @app.delete('/projects/<project_name>')
    @app.doc(
        tags=['Project Management-Project Operations'],
        summary='Delete Project',
        description='Deletes a DBT project and its warehouse data based on provided parameters.'
    )
    @app.auth_required(auth)
    @app.input(DeleteProjectParamsSchema, location='query')
    @app.output(DeleteProjectResponseSchema)
    def delete_project(project_name, query_data):
        """Delete a DBT project and/or its data based on input parameters
        
        Args:
            project_name (str): Name of the project to delete
            query_data (dict): Validated query parameters containing delete options
            
        Returns:
            dict: Deletion operation results including status and timestamp
            
        Raises:
            HTTPError(404): If project is not found
            HTTPError(500): If deletion operation fails
        """
        try:
            success, deletion_status = project_manager.delete_project(
                project_name,
                delete_data=query_data['delete_data'],
                delete_folder=query_data['delete_folder']
            )
            
            if not success:
                abort(404, message=f"Project {project_name} not found")
            
            # Construct appropriate message based on what was actually deleted
            message_parts = []
            if deletion_status.get('folder_deleted'):
                message_parts.append("project folder")
            if deletion_status.get('data_deleted'):
                message_parts.append("warehouse data")
            
            if not message_parts:
                message = f"No changes made to project {project_name}"
            else:
                message = f"Successfully deleted {' and '.join(message_parts)} for project {project_name}"
            
            return {
                'message': message,
                'project_name': project_name,
                'folder_deleted': deletion_status.get('folder_deleted', False),
                'data_deleted': deletion_status.get('data_deleted', False),
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            abort(500, message=f"Failed to delete project: {str(e)}")
    
    @app.delete('/projects/<project_name>/clean')
    @app.doc(
        tags=['Project Management-Analytics'],
        summary='Clean dbt Project',
        description='Cleans the DBT project by removing the target folder using dbt clean command.'
    )
    @app.auth_required(auth)
    def clean_project(project_name):
        """Clean DBT project target folder"""
        try:
            success = metadata_manager.clean_dbt_project(project_name)
            if not success:
                abort(500, message=f"Failed to clean project {project_name}")
                
            return {
                'project_name': project_name,
                'message': f'Successfully cleaned project {project_name}',
                'status': 'success'
            }
        except Exception as e:
            abort(500, message=str(e))