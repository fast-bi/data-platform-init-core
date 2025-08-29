-- Removes tables and views from the given run configuration for Redshift
-- Usage in production:
--    dbt run-operation cleanup_dbt_dataset
-- To only see the commands that it is about to perform:
--    dbt run-operation cleanup_dbt_dataset --args '{"dry_run": True}'
{% macro cleanup_dbt_dataset(dry_run=False) %}
    {% if execute %}
        {% set current_model_locations={} %}

        {% for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "seed", "snapshot"]) %}
            {% if not node.database in current_model_locations %}
                {% do current_model_locations.update({node.database: {}}) %}
            {% endif %}
            {% if not node.schema in current_model_locations[node.database] %}
                {% do current_model_locations[node.database].update({node.schema: []}) %}
            {% endif %}
            {% set table_name = node.alias if node.alias else node.name %}
            {% do current_model_locations[node.database][node.schema].append(table_name) %}
        {% endfor %}
    {% endif %}

    {% set cleanup_query %}

        with models_to_drop as (
            {% for database in current_model_locations.keys() %}
                {% if loop.index > 1 %}union all{% endif %}
                {% for schema, tables in current_model_locations[database].items() %}
                    {% if loop.index > 1 %}union all{% endif %}
                    select
                        table_type,
                        table_catalog as database_name,
                        table_schema as schema_name,
                        table_name,
                        case
                            when table_type = 'BASE TABLE' then 'TABLE'
                            when table_type = 'VIEW' then 'VIEW'
                        end as relation_type,
                        quote_ident(table_catalog) || '.' || quote_ident(table_schema) || '.' || quote_ident(table_name) as relation_name,
                        quote_ident(table_catalog) || '.' || quote_ident(table_schema) as schema_relation_name
                    from information_schema.tables
                    where table_schema = '{{ schema }}'
                      and table_name in ('{{ "', '".join(tables) }}')
                {% endfor %}
            {% endfor %}
        ),
        drop_commands as (
            select 'DROP ' || relation_type || ' ' || relation_name || ';' as command
            from models_to_drop
            union all
            select 'DROP SCHEMA IF EXISTS ' || schema_relation_name || ' CASCADE;' as command
            from models_to_drop
        )

        select DISTINCT command
        from drop_commands
        order by 1 desc;

    {% endset %}
    {% set drop_commands = run_query(cleanup_query).columns[0].values() %}
    {% if drop_commands %}
        {% for drop_command in drop_commands %}
            {% do log(drop_command, True) %}
            {% if dry_run | as_bool == False %}
                {% do run_query(drop_command) %}
            {% endif %}
        {% endfor %}
    {% else %}
        {% do log('No relations to clean.', True) %}
    {% endif %}
{%- endmacro %}
