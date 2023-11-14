/* 
Credit @stkbailey
This macro will apply a 'where' or 'limit' clause to models that are not
being run in production as a way of reducing the table size / run time of
tables. It works by checking what the `target.database` is, and implementing
minimization logic (either via "datediff" or "limit") when the target database
is not the production database.
*/
{% macro minimize_dev_table_size(
    production_database_name=none,
    date_column=none,
    day_limit=7,
    row_limit=10000,
    include_where_clause=True
) -%}

{%- if production_database_name is none -%}
    {% set production_database_name = var('database_prod') -%}
    {{ log("Production database name not set. Set to " ~ production_database_name) }}
{%- endif -%}

{%- if target.database|lower == production_database_name|lower -%}
    {{ log("Running in production. Ignoring minimization logic.") }}
{%- else -%}
    {%- if date_column is not none -%}
        {% if invocation_args_dict.which in ['build', 'run'] %}
        {{ log("Running in development. Adding " ~ day_limit ~"-day date filter to model: " ~ this.name, info=True) }}
        {% else %}
        {{ log("Running in development. Adding " ~ day_limit ~"-day date filter to model: " ~ this.name) }}
        {% endif %}
        {% if include_where_clause %}
            where
        {% endif %}
            datediff('days', {{date_column}} :: date, current_date()) < {{ day_limit }}
    {%- else -%}
        {% if invocation_args_dict.which in ['build', 'run'] %}
        {{ log("Running in development. Adding " ~ row_count ~ " row limit to model: " ~ this.name, info=True) }}
        {% else %}
        {{ log("Running in development. Adding " ~ row_count ~ " row limit to model: " ~ this.name) }}
        {% endif %}
        limit {{ row_count }}
    {%- endif -%}
{%- endif -%}
{% endmacro %}
