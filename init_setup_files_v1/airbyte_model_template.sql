{{ config(
    materialized="incremental",
     partition_by={
      "field": "execution_date",
      "data_type": "date",
      "granularity": "month"
    },
    re_data_time_filter="execution_date",
    incremental_strategy="merge",
    unique_key=["unique_id"],
    depends_on=["source_table_name"]
) }}

{%- if execute -%}
{{ generate_columns_from_airbyte_yml(source_dataset = "source_dataset_name",
                             source_table = "source_table_name",
                             model_name = "table_name",
                             unique_key = "unique_key_list") }}
{% endif %}