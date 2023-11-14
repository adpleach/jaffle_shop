/*
Macro to create a new table in the SCRATCH.SANBOX schema based on existing table in scratch or custom SQL run against a production table.
Designed for use with `clone_new_table_schema()` macro. Only available for use in development environment.

**TO USE CUSTOM SQL UPDATE custom_table_query MACRO AT THE BOTTOM OF THIS FILE**
**CUSTOM SQL SHOULD BE RUN AGAINST PRODUCTION DATABASE ("PROD") TO ENSURE DATA IS IN ITS MOST RECENT FORM**

**TO USE CLONE SCRATCH RUN NEW MODEL IN SCRATCH AND VALIDATE RESULTING DATA PRIOR TO RUNNING OPERATION**
**ENSURE minimize_dev_table_size() MACRO OR EQUIVALENT IS NOT USED IF CLONING SCRATCH TABLE**

To run use the following syntax:
  dbt run-operation create_new_table_schema --args "{create_type: 'clone_scratch', table_name: 'al_test'}"
Variables:
  create_type:
    'clone_scratch' or 'run_model'.
    Use clone_scratch to clone a rebuilt model from the SCRATCH database.
    Use run_model to create a table base on custom SQL run against production models
  table_name:
    The name of the table to be cloned or used as a base for the custom SQL.
*/

{% macro create_new_table_schema(
    create_type
    , table_name
  ) 
%} 

  -- {# Check variables for compatibility #}
  {% if target.database|lower != 'scratch' %}      
    {% do exceptions.raise_compiler_error('This operation is designed to be run in the scratch development environment only.') %}
  {% endif %}
  {% if create_type|lower != 'clone_scratch' and create_type|lower != 'run_model' %}      
    {% do exceptions.raise_compiler_error('Operation only available for create_type clone_scratch or run_model') %}
  {% endif %}
  {% set get_table_info = run_query("select table_type, is_transient, DATE_PART(epoch_second, systimestamp()) from "~target.database~".information_schema.tables where table_schema <> 'SANDBOX' and table_name = '"~table_name|upper~"';") %}
  {% if get_table_info[0][0] == 'VIEW' %}
    {% do exceptions.raise_compiler_error('Operation not available for views') %}
  {% endif %}

  -- {# Set variables for use in SQL #}
  {% if get_table_info[0][1] == 'YES' %}{% set is_transient = 'transient' %}{% else %}{% set is_transient = '' %}{% endif %}
  {% set time_now = get_table_info[0][2] %}

  -- {# Clone scratch table #}
  {% if create_type == 'clone_scratch' %}
    {% do log('Cloning target table '~table_name~' to SCRATCH.SANDBOX.'~table_name~'_'~time_now~'. Cloned table will be available for 30 days.'
      , info=true) %}
    {% set clone_table_query %}
      use role {{target.role}};
      create or replace {{is_transient}} table scratch.sandbox.{{table_name}}_{{time_now}} clone {{ref(table_name)}};
    {% endset %}
    {% do run_query(clone_table_query) %}
    -- {# Verify new table exists #}
    {% set new_table = run_query('SELECT * FROM SCRATCH.SANDBOX.'~table_name~'_'~time_now~' LIMIT 100;') %}
    {% if new_table|length == 0 %}
      {% do exceptions.raise_compiler_error('New table is empty.') %}
    {% endif %}
    {% do log('Table SCRATCH.SANDBOX.'~table_name~'_'~time_now~' has been created.', info=true) %}

  -- {# Run custom SQL #}
  {% else %}
    {% set new_table_name = ''~table_name~'_'~time_now~'' %}
    {% do log('Creating new table at SCRATCH.SANDBOX.'~new_table_name~' from custom SQL query. Cloned table will be available for 30 days.'
      , info=true) %}
    {% call statement('custom_query', fetch_result=True) %}
        {{ custom_table_query(new_table_name) }}
    {% endcall %}
    -- {# Verify new table exists #}
    {% set new_table = run_query('SELECT * FROM SCRATCH.SANDBOX.'~new_table_name~' LIMIT 100;') %}
    {% if new_table|length == 0 %}
      {% do exceptions.raise_compiler_error('New table is empty.') %}
    {% endif %}
    {% do log('Table SCRATCH.SANDBOX.'~new_table_name~' has been created.', info=true) %}

  {% endif %}

{% endmacro %}

{% macro custom_table_query(table_name) %}
  create or replace table scratch.sandbox.{{table_name}} as (
  -----------------------------------------------------------------------------------------------
  -- {# REPLACE SQL BELOW WITH CUSTOM TABLE QUERY #}
  -- {# USE {{ var('database_prod') }} OR "PROD" AS DATABASE REFERENCE #}
    with new_cte as (
        select
            cte.field_id
            , sum(cte.amount) as new_amount
        from {{ var('database_prod') }}.schema_name.new_amount_table cte
        where 1=1 
            and lower(cte.filter_field) = 'filter_value'
        group by 1
    )
    
    select bt.* exclude (cost_amount, cost_amount_dollars)
      , (bt.cost_amount + coalesce(cte.new_amount, 0))::number(38,8) as cost_amount
      , bt.cost_amount_dollars + coalesce(cte.new_amount/100.0, 0)::number(38,8)  as cost_amount_dollars
    from {{ var('database_prod') }}.schema_name.base_table bt
    left join new_cte cte
      on bt.field_id = cte.field_id
  -- {# END SQL REPLACEMENT #}
  -----------------------------------------------------------------------------------------------
  );
{% endmacro %}