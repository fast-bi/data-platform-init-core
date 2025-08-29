{% macro is_model_contains_ml(model_name) -%}
  {%- set contains_ml = False -%}
  {%- set ml_keywords = ['CREATE MODEL', 'PREDICT', 'TRAIN', 'ML.'] -%}
  {%- for node in graph.nodes -%}
    {%- if graph.nodes[node].name == model_name -%}
      {%- set model_sql = graph.nodes[node].raw_code -%}
    {%- endif -%}
    {%- for item in ml_keywords -%}
      {%- if item in model_sql -%}
        {%- set contains_ml = True -%}
        {%- do return(contains_ml) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endfor -%}
  {{- contains_ml -}}
{%- endmacro %}

{% macro source(source_name, table_name) -%}
  {%- set sql_source = builtins.source(source_name, table_name) -%}
  {%- set model_has_ml = is_model_contains_ml(this.name) -%}
  {%- if target.type == 'bigquery' -%}
    {%- if target.name == 'test' or target.name == 'e2e' or target.name == 'e2e_test' -%}
      {%- if model_has_ml != True -%}
        {%- set table_name_only = sql_source.identifier -%}
        {%- set sql_source = "`"+target.database+"."+target.dataset+"_"+source_name+"."+table_name_only+"`" -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
  {{- sql_source -}}
{%- endmacro %}