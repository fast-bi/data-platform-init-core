{% macro generate_columns_from_airbyte_yml(source_dataset, source_table, model_name, unique_key=None, date_from=None, partition_by_col=None, data_format=None) %}

{%- set main_col = '_airbyte_data'-%}
{%- set main_date_col = '_airbyte_emitted_at'-%}
{%- set main_date_col_target_table = 'ab_emitted_at'-%}
{%- set skip_columns = ['unique_id'] -%}
{%- set eliminate_ab_columns = ['ab_emitted_at', 'ab_id', 'execution_date'] -%}
{%- if partition_by_col -%}
    {%- if data_format -%}
    {%- set date_col = "PARSE_DATE('" ~data_format~ "', JSON_EXTRACT_SCALAR(_airbyte_data, '$." ~ partition_by_col ~"'))" -%}
    {%- else %}
    {%- set date_col = "CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$." ~ partition_by_col ~"') AS timestamp)" -%}
    {%- endif -%}
{%- else -%}
    {%- set date_col = "execution_date" -%}
    {%- set partition_by_col = "execution_date" -%}
{%- endif -%}
{%- set ns = namespace(matched = 0) %}
WITH latest_source_data AS(
{% if not is_incremental() %}
SELECT * except(rn)
FROM (
    SELECT *,
    row_number() over (partition by {{main_col}} order by {{ main_date_col }} desc) as rn
    FROM {{ source(var(source_dataset), source_table) }}
    WHERE DATE({{main_date_col}}) <= DATE('{{ var("execution_date") }}')
    {% if date_from %}
    AND DATE({{main_date_col}}) >= DATE("{{date_from}}")
    {%- endif -%}
)
WHERE rn = 1
{%-else-%}
SELECT * except(rn)
FROM (
    SELECT *,
    row_number() over (partition by {{main_col}} order by {{ main_date_col }} desc) as rn
    FROM {{ source(var(source_dataset), source_table) }}
    WHERE DATE({{main_date_col}}) >= DATE_SUB(DATE('{{ var("execution_date") }}'), INTERVAL 1 DAY)
    )
WHERE rn = 1
{% endif %}
)
, unnested_json_col AS (
    SELECT
        {% for node in graph.nodes.values() %}
            {%- if node.name == model_name -%}
                {%- for col_dict in node.columns.values() -%}
                    {%- if col_dict.name not in skip_columns -%}
                        {%- if col_dict.name not in eliminate_ab_columns -%}
                        {%- if col_dict.name == 'ab_cdc_cursor' %}
                        {%- set ns.matched = 1 %}
                        {% endif %}
                        {%- if col_dict.data_type == 'timestamp'-%}
                                {% set tm = "JSON_EXTRACT_SCALAR(" ~ main_col ~ ", '$." ~ col_dict.identifier ~ "')" %}
                                CASE
                                    WHEN LENGTH({{tm}}) = 8 THEN PARSE_TIMESTAMP('%Y%m%d', {{tm}})
                                    WHEN LENGTH({{tm}}) = 10 THEN PARSE_TIMESTAMP('%Y-%m-%d', {{tm}})
                                    WHEN LENGTH({{tm}}) < 2 THEN CAST(NULL AS timestamp)
                                    ELSE CAST({{tm}} AS timestamp)
                                END AS {{col_dict.name}}
                                {%- if not loop.last -%},{% endif %}
                            {%- elif col_dict.data_type == 'array'-%}
                                ARRAY_TO_STRING(JSON_EXTRACT_ARRAY({{main_col}}, '$.{{col_dict.identifier}}'),"") AS {{col_dict.name}}
                                {%- if not loop.last -%},{% endif %}
                            {%- else -%}
                                CAST(JSON_EXTRACT_SCALAR({{main_col}}, '$.{{col_dict.identifier}}') AS {{col_dict.data_type}}) AS `{{col_dict.name}}`
                                {%- if not loop.last -%},{% endif %}
                            {%- endif -%}
                        {% elif col_dict.identifier == "execution_date" -%}
                            DATE('{{ var("execution_date") }}') AS {{col_dict.name}}
                            {%- if not loop.last -%},{% endif -%}
                        {% else -%}
                            {{col_dict.identifier}} AS {{col_dict.name}}
                            {%- if not loop.last -%},{%- endif -%}
                        {% endif %}
                    {% endif %}
                {%- endfor %}
            {%- endif %}
        {%- endfor -%}
    FROM latest_source_data
)

{% if unique_key %}
, result AS (
    {%- set key_list = unique_key.split(', ') %}
    {%- set formatted_key_list = [] -%}
    {%- for key in key_list -%}
        {%- set _ = formatted_key_list.append('`' + key + '`') -%}
    {% endfor -%}

    {%- if key_list|length > 1 %}
        SELECT *
        FROM (
                SELECT *, {{ dbt_utils.generate_surrogate_key(formatted_key_list) }} AS unique_id
                FROM unnested_json_col
        )
    {%- else %}
        SELECT *
        FROM (
            SELECT *,
                   {{unique_key}} AS unique_id
            FROM unnested_json_col
        )

    {%- endif %}
)
--temp
{%- if ns.matched == 1 %}
SELECT * except(rn)
FROM ( SELECT *,
       row_number() over (partition by unique_id order by ab_cdc_updated_at desc) as rn
       FROM result)
WHERE rn = 1
--temp2
{%- elif ns.matched == 0 %}
SELECT * FROM result
{%- endif %}

{% else %}
    SELECT * FROM unnested_json_col
{%- endif %}

{% endmacro %}
