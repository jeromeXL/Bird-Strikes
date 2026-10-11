{#
    Use the schema name exactly as configured (e.g. STAGING).
    dbt's default would prefix it with the target schema, giving STAGING_STAGING.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
