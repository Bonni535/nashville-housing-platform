{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {# No schema override — use the profile's default schema (RAW) #}
        {{ default_schema }}

    {%- else -%}
        {# Use the schema name exactly as specified in dbt_project.yml.
           This ensures staging/ → STAGING, intermediate/ → INTERMEDIATE, marts/ → MARTS
           with NO target-name prefix (i.e. not DEV_STAGING or PROD_MARTS). #}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
