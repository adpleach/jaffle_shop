/*
Macro to run a delete from statement to remove a subset of rows from an existing table in the products or projects schemas.
**USE WITH CAUTION. RUNNING THIS STATEMENT WILL REMOVE ROWS FROM THE TABLE AT THE LOCATION SPECIFIED**
**ALWAYS TEST WITH A DRY RUN BEFORE PROCEEDING**

To run use the following syntax:
  dbt run-operation delete_from_table --args "{table_name: 'al_test', schema_name: 'projects'}"
  dbt run-operation delete_from_table --args "{table_name: 'al_test', schema_name: 'projects', dry_run: False, where_clause: \"1=1 and date(event_timestamp) >= '2023-04-01'\"}"

Variables:
  table_name:
    The name of the table to be created or replaced.
  schema_name:
    The name of the schema in which the table is located.
  where:
    Default 1=1
    Please provide in the following format - 
      \"1=1 and date(event_timestamp) >= '2023-04-01'\"
  dry_run:
    Default is true
    If true, returns the SQL to be run in the warehouse. If false, runs the command.
*/

{% macro delete_from_table(
    table_name
    , schema_name
    , where_clause = '1=1'
    , dry_run = True
  ) 
%} 

  {% if schema_name|lower != 'products' and schema_name|lower != 'projects' %}
    {% do exceptions.raise_compiler_error('Operation only available for schema_name products or projects') %}
  {% endif %}

  {% if dry_run %}

    {% call statement('row_count', fetch_result=True) %}
      select count(*) as cnt from {{target.database}}.{{schema_name}}.{{table_name}} where {{ where_clause }};
    {% endcall %}

    {%- set row_count = load_result('row_count')['data'][0][0] -%}
    
    {% if row_count == 0 %}
      {% do log('No records match the where clause given', info=true) %}
    {% else %}
      {% do log('The following SQL will be run:
        delete from '~target.database~'.'~schema_name~'.'~table_name~' where '~where_clause~';
        '~row_count~' rows will be deleted', info=true) 
      %}
    {% endif %}
  
  {% else %}

    {% call statement('before_row_count', fetch_result=True) %}
      select count(*) as cnt from {{target.database}}.{{schema_name}}.{{table_name}} where {{ where_clause }};
    {% endcall %}

    {%- set before_row_count = load_result('before_row_count')['data'][0][0] -%}
    
    {% if before_row_count == 0 %}
      {% do log('No records match the where clause given', info=true) %}

    {% else %}
      {% do log('Running the following SQL:
        delete from '~target.database~'.'~schema_name~'.'~table_name~' where '~where_clause~';
        '~before_row_count~' rows are being deleted.', info=true) 
      %}
      {% call statement() -%}
        delete from {{target.database}}.{{schema_name}}.{{table_name}} where {{ where_clause }};
      {%- endcall %}

      {% call statement('after_row_count', fetch_result=True) -%}
        select count(*) as cnt from {{target.database}}.{{schema_name}}.{{table_name}} where {{ where_clause }};
      {%- endcall %}

      {%- set after_row_count = load_result('after_row_count')['data'][0][0] -%}

      {% if after_row_count != 0 %}
        {% do exceptions.raise_compiler_error('Records remain that match the where clause. 
        Please ensure no operations are currently active on the table prior to running again') %}
      {% else %}
        {% do log(''~after_row_count~' records remain that match the where clause given. Process complete.', info=true) %}
      {% endif %}

    {% endif %}

  {% endif %}

{% endmacro %}