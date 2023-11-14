/*
Written by @lalita-whatnot
    ts:
        the timestamp that is used to determine new records
    last_run_ts:
        the timestamp that indicates the last run time
    include_buffer: 
        default is false.
        make true if we just want to continuously rerun the last x time part of data for every run
    buffer_interval_part: 
        days/hours/minutes
    buffer_interval:
        the number of parts
*/
{%- macro incremental_partition_filter(
    ts
    , last_run_ts
    , include_buffer = false
    , buffer_interval = 1
    , buffer_interval_part = 'hour'
) -%}
{% set default_last_run_ts = '(select max(th.'~last_run_ts~') from '~this~' th)' %}
{% set default_last_run_ts_with_buffer = "(select max(th."~last_run_ts~") - interval '"~buffer_interval~" "~buffer_interval_part~"' from "~this~" th)" %}
    {% if include_buffer %}
        and {{ var('backfill_ts_field' , ts) }} >= {{ var('start_ts' , default_last_run_ts_with_buffer) }}
    {% else %}
        and {{ var('backfill_ts_field' , ts) }} >= {{ var('start_ts' , default_last_run_ts) }}
    {% endif %}
        and {{ var('backfill_ts_field' , ts) }} < {{ var('end_ts' , 'sysdate()' )}}
{%- endmacro -%}