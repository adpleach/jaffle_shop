/* Written by @atl-k */
{% materialization unload, adapter='snowflake' %}
    {% set unload_stage_fqn = config.get('unload_stage_fqn') %}
    {% set s3uri_prefix = config.get('s3uri_prefix') %}
    {% set run_datetime = run_started_at.strftime("%Y/%m/%d/%H/%M/%S") %}
    {% set max_file_size = config.get('max_file_size', 16777216)|int %}
    {% set file_format_type = config.get('file_format_type', 'json') %}
    {% set compression = config.get('compression', 'NONE') %}
    {% set header = config.get('header', false) %}
    {% set success_file = config.get('success_file', false) %}

-- Execute any sql required to implement the desired materialization
    {%- call statement('main') -%}
        begin;
        COPY INTO @{{ unload_stage_fqn }}/{{ s3uri_prefix }}/{{ this.database|lower }}/{{ this.schema }}/{{ this.identifier }}/{{ run_datetime }}/{{ env_var("DBT_CLOUD_RUN_ID", "local_run") }}/data
        {{ sql }}
        file_format = (type={{ file_format_type }}, COMPRESSION={{ compression }})
        header = {{ header }}
        MAX_FILE_SIZE = {{ max_file_size }};
        commit;
    {% if success_file %}
        begin;
        COPY INTO @{{ unload_stage_fqn }}/{{ s3uri_prefix }}/{{ this.database|lower }}/{{ this.schema }}/{{ this.identifier }}/{{ run_datetime }}/{{ env_var("DBT_CLOUD_RUN_ID", "local_run") }}/SUCCESS
        FROM (
            SELECT OBJECT_CONSTRUCT(
                'current_timestamp', current_timestamp(),
                'status', 'it was written'
            )
        )
        file_format = (type=JSON, COMPRESSION=NONE);
        commit;
    {% endif %}


    {%- endcall -%}
    {{ return({'relations': []}) }}
{% endmaterialization %}
