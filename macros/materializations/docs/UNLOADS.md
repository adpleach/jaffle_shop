# Unload Materialization

This materialization handles unloading data from a model to an external stage. In order to use this, an external stage should exist in snowflake. 
Please ensure all models using this macro have the "unload" tag in the config block this will exclude the model from the CI run which is not configured to run unload workflows.

## Boilerplate

```hcl
{{
config(
materialized='unload',
unload_stage_fqn='DB_NAME.SCHEMA_NAME.TABLE_NAME',
s3uri_prefix = 'test_unload',
max_file_size = 16777216,
file_format_type = 'json',
compression = 'none',
)
}}

FROM (
SELECT object_construct(*)
FROM {{ ref('some_model') }}
)
```

| Variable         | Required | Default | Definition                                                 |
|------------------|----------|---------|------------------------------------------------------------|
| unload_stage_fqn | Yes      | None    | Fully qualified name of the Stage used for unloading data to |
| s3uri_prefix        | Yes      | None    | Prefix to add in s3 uri for differitiating unloads         |
| max_file_size | No | 16777216 | Max file size wanted during unload (in bytes) |
| file_format_type | No | json | File type (either JSON or Parquet) |
| compression | No | None | File compression type - value depends on file_format_tyep (see docs) |


### Tip: When unloading to json file format, rows must be in an object type - so if you are doing a select * from a table, ensure it's like something below:

```
FROM (
SELECT object_construct(*)
FROM {{ ref('some_model') }}
)
```

### For parquet, you can do the typical select * in your unload sql. 


## Resources

- https://docs.snowflake.com/en/user-guide/data-unload-s3.html
- https://docs.snowflake.com/en/sql-reference/sql/copy-into-location.html
