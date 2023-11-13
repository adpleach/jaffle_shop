/*
Macro to replace a production table in the products or projects schemas with a table created in the SCRATCH.SANDBOX schema. 
This can be used to update the schema of a production table without running a full-refresh.
The table should be created in SCRATCH.SANDBOX prior to running this macro.

To rename a column add it to the drop_columns list and add_columns dictionary.

To revert the process - run again using the name of the cloned target table as the source table.

**USE WITH CAUTION. RUNNING THIS STATEMENT WILL REPLACE THE TABLE OR REMOVE/INSERT RECORDS INTO THE TABLE AT THE LOCATION SPECIFIED**
**ALWAYS TEST WITH A DRY RUN AND THOROUGLY VALIDATE DATA IN THE SOURCE TABLE BEFORE PROCEEDING**

To run use the following syntax:
  dbt run-operation clone_new_table_schema --args "{target_table_name: 'al_prod', target_schema_name: 'projects', source_table_name: 'al_test'}"
  dbt run-operation clone_new_table_schema --args "{target_table_name: 'al_prod', target_schema_name: 'projects', source_table_name: 'al_test', dry_run: False, drop_columns: ['old_column1', 'old_column2'], add_columns: {new_column1: varchar, new_column2: number}}"

Variables:
  source_table_name:
    The name of the table to be created or replaced.
  target_table_name:
    The name of the table to be created or replaced.
  target_schema_name:
    The name of the schema in which the table is located.
  drop_columns: (List)
    List of columns to be dropped from the existing table (only required if data_retention_time >= 5 days)
  add_columns: (Dictionary)
    Dictionary containing the name of each column to be added and the data type
    e.g. {column_name1: varchar, column_name2: number}
  large_row_count_diff:
    Default is false
    If false, will raise exception if row count of source and target tables differs by > 10%
  dry_run:
    Default is true
    If true, returns the SQL to be run in the warehouse. If false, runs the command.
*/

{% macro clone_new_table_schema(
    source_table_name
    , target_table_name
    , target_schema_name
    , drop_columns = None
    , add_columns = None
    , large_row_count_diff = False
    , dry_run = True
  ) 
%} 
  -- {# Check source table exists #}
  {% set source_table_exists = run_query("select * from scratch.information_schema.tables where table_schema = 'SANDBOX' and table_name = '"~source_table_name|upper~"'") %}
  {% if source_table_exists|length == 0 %}
    {% do exceptions.raise_compiler_error('Source table does not exist.') %}
  {% endif %}
  -- {# Check source table contains records #}
  {% set source_table_length = run_query('select * from scratch.sandbox.'~source_table_name~' limit 100;') %}
  {% if source_table_length|length == 0 %}
    {% do exceptions.raise_compiler_error('Source table does not contain any records.') %}
  {% endif %}
  -- {# Check number source table records is not too different from target table #}
  {% set target_table_count = run_query('select count(*) from '~target.database~'.'~target_schema_name~'.'~target_table_name~';') %}
  {% set source_table_count = run_query('select count(*) from scratch.sandbox.'~source_table_name~';') %}
  {% if large_row_count_diff == False and ((source_table_count[0][0]-target_table_count[0][0])/target_table_count[0][0])|abs > 0.1 %}
    {% do exceptions.raise_compiler_error('Row count of source table differs > 10% from target table. Please verify this is correct and set large_row_count_diff to True.') %}
  {% endif %}
  -- {# Only allow products or projects #}
  {% if target_schema_name|lower != 'products' and target_schema_name|lower != 'projects' %}
    {% do exceptions.raise_compiler_error('Operation only available for schema_name products or projects') %}
  {% endif %}

  -- {# Verify object type #}
  {% call statement('get_table_type', fetch_result=True) %}
    select table_type, is_transient from {{target.database}}.information_schema.tables where table_name = '{{target_table_name|upper}}';
  {% endcall %}
  {%- set results = load_result('get_table_type')['data'] -%}
  {%- set table_type = results[0][0] -%}
  {%- set target_transient = results[0][1] -%}
  {% if table_type == 'VIEW' %}
    {% do exceptions.raise_compiler_error('Operation not available for views') %}
  {% endif %}
  {% if target_transient == 'YES' %}{% set target_is_transient = 'transient' %}{% else %}{% set target_is_transient = '' %}{% endif %}

  -- {# Verify last_altered #}
  {% set last_altered = run_query("select DATE_PART(epoch_second, last_altered), is_transient from scratch.information_schema.tables where table_schema = 'SANDBOX' and table_name = '"~source_table_name|upper~"'") %}
  {% set time_now = run_query('select DATE_PART(epoch_second, systimestamp());') %}
  {% if (time_now[0][0] - last_altered[0][0]) > 86400 %}
    {% do exceptions.raise_compiler_error('Source table has not been altered for > 24 hours. Please make sure it is up to date.') %}
  {% endif %}
  {% if last_altered[0][1] == 'YES' %}{% set source_is_transient = 'transient' %}{% else %}{% set source_is_transient = '' %}{% endif %}

  -- {# Get retention time #}
  {% call statement('get_retention_time', fetch_result=True) %}
    show parameters like '%DATA_RETENTION_TIME_IN_DAYS%' in table {{target.database}}.{{target_schema_name}}.{{target_table_name}};
  {% endcall %}
  {%- set retention_time = load_result('get_retention_time')['data'][0][1] -%}

  -- {# Get final table column names #}
  {% set column_query = run_query('show columns in '~target.database~'.'~target_schema_name~'.'~target_table_name~';') %}
  {% set column_names = [] %}
  {% for i in range(column_query|length) %}
    {{column_names.append(column_query[i][2])}}
  {% endfor %}
  {% if add_columns %}
    {% set add_query = [] %}
    {% for key, value in add_columns.items() %}
      {{column_names.append(key|upper)}}
    {% endfor %}
  {% endif %}
  {% if drop_columns %}
    {% for column in drop_columns %}
      {% if column|upper in column_names %}
      {{column_names.remove(column|upper)}}
      {% endif %}
    {% endfor %}
  {% endif %}

  -- {# Validate final table column names match source table names #}
  {% set source_column_query = run_query('show columns in scratch.sandbox.'~source_table_name~';') %}
  {% set source_column_names = [] %}
  {% for i in range(source_column_query|length) %}
    {{source_column_names.append(source_column_query[i][2])}}
  {% endfor %}
  {% for column in column_names %}
    {% if column|upper in source_column_names %}
    {{source_column_names.remove(column|upper)}}
    {% endif %}
  {% endfor %}
  {% if source_column_names|length != 0 %}
    {% do exceptions.raise_compiler_error('Columns in source table do not match target table. Please verify column names provided.') %}
  {% endif %}

  -- {# dry_run to validate SQL #}
  {% if dry_run %}
    {% if retention_time|int < 1 %} 
      {% do log('The following SQL will be run:
        use role '~target.role~';
        create or replace '~source_is_transient~' table '~target.database~'.'~target_schema_name~'.'~target_table_name~' clone scratch.sandbox.'~source_table_name~';'
        , info=true) 
      %}
    {% else %}
      {% if add_columns %}
        {% set add_query = [] %}
        {% for key, value in add_columns.items() %}
          {{add_query.append('alter table '~target.database~'.'~target_schema_name~'.'~target_table_name~' add '~key~' '~value~';')}}
        {% endfor %}
      {% endif %}
      {% if drop_columns %}
        {% set drop_query = 'alter table '~target.database~'.'~target_schema_name~'.'~target_table_name~' drop column if exists '~drop_columns|join(', ')~';' %}
      {% endif %}
      {% do log('The following SQL will be run:
        use role '~target.role~';
        truncate table '~target.database~'.'~target_schema_name~'.'~target_table_name~';
        '~drop_query~'
        '~add_query|join('\n')~'
        insert into '~target.database~'.'~target_schema_name~'.'~target_table_name~' 
        select
          '~column_names|join(', ')~'
        from scratch.sandbox.'~source_table_name~';'
        , info=true) 
      %}
    {% endif %}
  
  -- {# Full run #}
  {% else %}
    -- {# Log and wait #}
    {% do log('Table '~target_table_name~' will be replaced by '~source_table_name~' in 30s. 
      Please ensure: 
        - dry_run has been performed
        - data in '~source_table_name~' has been validated
        - PR associated with the change has been merged
      Terminate this run now if you have not yet validated the source table.'
      , info=true) %}

    {% set wait_query %}
      CALL SYSTEM$WAIT(30);
    {% endset %}
    {% do run_query(wait_query) %}

    -- {# Clone target table into SANDBOX #}
    {% do log('Cloning target table '~target_table_name~' to SCRATCH.SANDBOX.'~target_table_name~'_'~time_now[0][0]~'. Cloned table will be available for 30 days.'
      , info=true) %}
    {% set clone_target_query %}
      use role snowflake_developer;
      create or replace {{target_is_transient}} table scratch.sandbox.{{target_table_name}}_{{time_now[0][0]}} clone {{target.database}}.{{target_schema_name}}.{{target_table_name}};
    {% endset %}

    {% do run_query(clone_target_query) %}
    
    -- {# If data retention is short - clone table to replace #}
    {% if retention_time|int < 1 %}    
      {% do log('Cloning source table SCRATCH.SANDBOX.'~source_table_name~' to '~target.database~'.'~target_schema_name~'.'~target_table_name~'.'
        , info=true) %}
      {% set clone_source_query %}
        use role {{target.role}};
        create or replace {{source_is_transient}} table {{target.database}}.{{target_schema_name}}.{{target_table_name}} clone scratch.sandbox.{{source_table_name}};
      {% endset %}

      {% do run_query(clone_source_query) %}

    -- {# If data retention is long - truncate, alter and insert #}
    {% else %}
      -- {# Truncate table #}
      {% set truncate_target_query %}
        use role {{target.role}};
        truncate table {{target.database}}.{{target_schema_name}}.{{target_table_name}};
      {% endset %}
      {% do log('Truncating table '~target.database~'.'~target_schema_name~'.'~target_table_name~'.', info=true) %}
      {% do run_query(truncate_target_query) %}

      -- {# Alter table #}
      {% if add_columns or drop_columns %}
      {% set alter_target_query %}
        use role {{target.role}};
        {% if drop_columns %}
        alter table {{target.database}}.{{target_schema_name}}.{{target_table_name}} drop column if exists {{ drop_columns | join(', ') }};
        {% endif %}
        {% if add_columns %}
          {%- for key, value in add_columns.items() -%}
          alter table {{target.database}}.{{target_schema_name}}.{{target_table_name}} add {{key}} {{value}};
          {%- endfor -%}
        {% endif %}
      {% endset %}
      {% do log('Altering table '~target.database~'.'~target_schema_name~'.'~target_table_name~'.', info=true) %}
      {% do run_query(alter_target_query) %}
      {% endif %}

      -- {# Insert records #}
      {% set insert_target_query %}
        use role {{target.role}};
        insert into {{target.database}}.{{target_schema_name}}.{{target_table_name}}
        select
          {{ column_names|join(', ')}}
        from scratch.sandbox.{{source_table_name}};
      {% endset %}
      {% do log('Inserting rows into table '~target.database~'.'~target_schema_name~'.'~target_table_name~'.', info=true) %}
      {% do run_query(insert_target_query) %}

    {% endif %}

  {% endif %}

{% endmacro %}
