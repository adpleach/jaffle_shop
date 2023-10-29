/*
This macro attempts to decode base encoded ids.
*/
{% macro try_decode_ids(column) -%}
    ifnull(
        regexp_replace(
            try_base64_decode_string({{ column }})
            ,'[^[:digit:]]'
            , ''
        ),
        {{ column }}
    )
{%- endmacro %}
