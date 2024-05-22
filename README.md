# dlt-auto

## Summary
This solution automatically generates required DLT resources (i.e., tables and view) based on the table configurations.

This is based on the metaprogramming that discussed in (https://docs.databricks.com/en/delta-live-tables/create-multiple-tables.html)

This can be combined with DABs to define environment dependent configs. DABs can modify the settings/advanced/configurations for differnt environemtns: e.g., evn: dev, source_databse: test_db, ... and these can be accessed in the code using spark.conf.get()
For example the source_databse can be changed using DABs or the code can behave differntly based on the env (can load differnt set of configs for example). 

source_database = spark.conf.get("mypipeline.source_database")


## The main components:

- @set_up_table decorator:  wraps a function with the required decorators to create the DLT resources.
- table_configurations: configuration of the resources. This can be updated by loading json configurations or using predefine functions.
- parent_templates: a config can "inherit" from a parent template. this parent template will have the common fields and helps avoiding repetition. For exmaple, the target_schema for all DLT tables and most source_tables is LIVE which is defined in the parent template. So unless a differnt source schema is used re-defining the source_schema is not needed.

  {
  "parent_template": append_only_template,
  "target_table_name": "bronze_table_1",
  "source_table_name": "streaming_source",
  "comment": "Bronze table from live data source",
}

## Codebase
- create_pipeline: the main loop that calls the right functions based on the config
- Databricks_temaplates: parent_templates (common fields for a specific type of resource)
- Databricks_functions: functions to create pre-defined DLT resources and the set_up_table decorator decorator
- custom_functions: users can define more complex functions to transform data by folloinwg the exmaple pattern here. The logic in create_table_df truns into a DLT resources using set_up_table decorator
- 

## Config
Each item in table configurations has different fields based on the type of the resource. 
There are some predefined types: append_only, upsert, sql_table.
These types have some pre-defined required fields and some fields with default values (which can be changed). 

For example for an append_only table: target_table_name and source_table_name are required. the default value for the schema of the source_table is LIVE. However this can be changed to read data from a delta table outside of the DLT pipeline by changing the schema to another catalog.schema

## Custom functions
It is also possible to create resources using custom functions. User can define these new functions by following a pattern specified in the custom_function.py. This rerouces must be of type CUSTOM

## Types
Example types so far are
- append_only: streaming table from one source (delta table)
- upsert: upsert data from one source (delta table)
- sql: can pass a fixed sql query or a parameterised one to create an MV
- custom: custom user function  

These can have expectations if they're defined in the config


## Next steps
- append flow
- json validations (similar to Erik's code)
- move the configuration to a separate file or files
- integrate with DABs


