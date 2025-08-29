import psycopg2
from psycopg2 import extras

class dbNavigator:
    def __init__(self, db_config):
        self.connection = psycopg2.connect(**db_config)
        self.data_catalog_table = 'data_catalog_metadata'
        self.data_quality_table = 'data_quality_metadata'
        self.user_groups_table = 'user_groups'
        self.resource_access_table = 'resource_access'
        self.ensure_tables_exist()

    def ensure_tables_exist(self):
        self.ensure_table_data_catalog()
        self.ensure_table_data_quality()
        self.ensure_user_groups_table()
        self.ensure_resource_access_table()

    def ensure_table_data_catalog(self):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.data_catalog_table} (
                    id SERIAL PRIMARY KEY,
                    avatars_url VARCHAR(255),
                    dbt_project_name VARCHAR(255) NOT NULL,
                    dbt_project_version VARCHAR(64) NOT NULL,
                    dbt_project_owner VARCHAR(255) NOT NULL,
                    dbt_project_tags VARCHAR(255),
                    dbt_project_endpoint_link VARCHAR(255) NOT NULL,
                    dbt_project_deployment_name VARCHAR(255),
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    online_status BOOLEAN DEFAULT TRUE
                );
            """)
            self.connection.commit()

    def ensure_table_data_quality(self):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.data_quality_table} (
                    id SERIAL PRIMARY KEY,
                    avatars_url VARCHAR(255),
                    dbt_project_name VARCHAR(255) NOT NULL,
                    dbt_project_version VARCHAR(64) NOT NULL,
                    dbt_project_owner VARCHAR(255) NOT NULL,
                    dbt_project_tags VARCHAR(255),
                    dbt_project_endpoint_link VARCHAR(255) NOT NULL,
                    dbt_project_deployment_name VARCHAR(255),
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    online_status BOOLEAN DEFAULT TRUE
                );
            """)
            self.connection.commit()

    def ensure_user_groups_table(self):
        with self.connection.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_groups (
                    group_id SERIAL PRIMARY KEY,
                    group_name VARCHAR(255) UNIQUE NOT NULL
                );
            """)
            self.connection.commit()

    def ensure_resource_access_table(self):
        with self.connection.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS resource_access (
                    access_id SERIAL PRIMARY KEY,
                    group_id INTEGER NOT NULL,
                    resource_id INTEGER NOT NULL,
                    access_type VARCHAR(50),  -- Examples: 'read', 'write', 'admin'
                    FOREIGN KEY (group_id) REFERENCES user_groups(group_id),
                    FOREIGN KEY (resource_id) REFERENCES data_catalog_metadata(id)
                );
            """)
            self.connection.commit()

    def get_data_catalog_metadata(self):
        with self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"SELECT * FROM {self.data_catalog_table};")
            return cur.fetchall()

    def get_data_quality_metadata(self):
        with self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"SELECT * FROM {self.data_quality_table};")
            return cur.fetchall()

    def get_data_catalog_metadata_by_id(self, id):
        with self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"SELECT * FROM {self.data_catalog_table} WHERE id = %s;", (id,))
            return cur.fetchone()
        
    def get_data_quality_metadata_by_id(self, id):
        with self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"SELECT * FROM {self.data_quality_table} WHERE id = %s;", (id,))
            return cur.fetchone()

    def get_data_catalog_metadata_by_dbt_project_name(self, dbt_project_name):
        with self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"SELECT * FROM {self.data_catalog_table} WHERE dbt_project_name = %s;", (dbt_project_name,))
            return cur.fetchone()
        
    def get_data_quality_metadata_by_dbt_project_name(self, dbt_project_name):
        with self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"SELECT * FROM {self.data_quality_table} WHERE dbt_project_name = %s;", (dbt_project_name,))
            return cur.fetchone()

    def add_data_catalog_metadata(self, data):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {self.data_catalog_table} (avatars_url, dbt_project_name, dbt_project_version, 
                dbt_project_owner, dbt_project_tags, dbt_project_endpoint_link, dbt_project_deployment_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
                """, (data['avatars_url'], data['dbt_project_name'], data['dbt_project_version'],
                      data['dbt_project_owner'], data['dbt_project_tags'], data['dbt_project_endpoint_link'], data['dbt_project_deployment_name']))
            self.connection.commit()
            return cur.fetchone()[0]  # Return the id of the newly created record

    def add_data_quality_metadata(self, data):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {self.data_quality_table} (avatars_url, dbt_project_name, dbt_project_version, 
                dbt_project_owner, dbt_project_tags, dbt_project_endpoint_link, dbt_project_deployment_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;
                """, (data['avatars_url'], data['dbt_project_name'], data['dbt_project_version'],
                      data['dbt_project_owner'], data['dbt_project_tags'], data['dbt_project_endpoint_link'], data['dbt_project_deployment_name']))
            self.connection.commit()
            return cur.fetchone()[0]

    def update_data_catalog_metadata(self, id, data):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                UPDATE {self.data_catalog_table}
                SET avatars_url = %s, dbt_project_name = %s, dbt_project_version = %s, 
                dbt_project_owner = %s, dbt_project_tags = %s, dbt_project_endpoint_link = %s, dbt_project_deployment_name = %s,
                updated_at = %s, online_status = %s
                WHERE id = %s;
                """, (
                    data['avatars_url'], data['dbt_project_name'], data['dbt_project_version'],
                    data['dbt_project_owner'], data['dbt_project_tags'], data['dbt_project_endpoint_link'], data['dbt_project_deployment_name'],
                    data['updated_at'], data['online_status'], id
                )
            )
            self.connection.commit()

    def update_data_quality_metadata(self, id, data):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                UPDATE {self.data_quality_table}
                SET avatars_url = %s, dbt_project_name = %s, dbt_project_version = %s, 
                dbt_project_owner = %s, dbt_project_tags = %s, dbt_project_endpoint_link = %s, dbt_project_deployment_name = %s,
                updated_at = %s, online_status = %s
                WHERE id = %s;
                """, (
                    data['avatars_url'], data['dbt_project_name'], data['dbt_project_version'],
                    data['dbt_project_owner'], data['dbt_project_tags'], data['dbt_project_endpoint_link'], data['dbt_project_deployment_name'],
                    data['updated_at'], data['online_status'], id
                )
            )
            self.connection.commit()

    def update_data_catalog_online_status_metadata(self, id, data):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                UPDATE {self.data_catalog_table}
                SET updated_at = %s, online_status = %s
                WHERE id = %s;
                """, (
                    data['updated_at'], data['online_status'], id
                )
            )
            self.connection.commit()

    def update_data_quality_online_status_metadata(self, id, data):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                UPDATE {self.data_quality_table}
                SET updated_at = %s, online_status = %s
                WHERE id = %s;
                """, (
                    data['updated_at'], data['online_status'], id
                )
            )
            self.connection.commit()

    def update_data_catalog_refresh_project_metadata(self, id, data):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                UPDATE {self.data_catalog_table}
                SET updated_at = %s, online_status = True
                WHERE id = %s;
                """, (
                    data['updated_at'], id
                )
            )
            self.connection.commit()

    def update_data_quality_refresh_project_metadata(self, id, data):
        with self.connection.cursor() as cur:
            cur.execute(f"""
                UPDATE {self.data_quality_table}
                SET updated_at = %s, online_status = True
                WHERE id = %s;
                """, (
                    data['updated_at'], id
                )
            )
            self.connection.commit()

    def delete_data_catalog_metadata(self, data):
        with self.connection.cursor() as cur:
            cur.execute(f"DELETE FROM {self.data_catalog_table} WHERE id = %s;", (data['id'],))
            self.connection.commit()

    def delete_data_quality_metadata(self, data):
        with self.connection.cursor() as cur:
            cur.execute(f"DELETE FROM {self.data_quality_table} WHERE id = %s;", (data['id'],))
            self.connection.commit()

