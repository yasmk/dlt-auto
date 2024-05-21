# dlt-auto

This solution automatically generates required DLT resurces (i.e., tables and view) based on the table configurations.

This is using metaprogramming as discussed in (https://docs.databricks.com/en/delta-live-tables/create-multiple-tables.html)

Each table configuration has different fields based on the type of the resource. 
There are some predefined types: append_only, upsert, sql_table.
These types have some pre-defined required fields and some fields with default values (which can be changed). 

For example for an append_only table: target_table_name and source_table_name are required. the default value for the schema of the source_table is LIVE. However this can be changed to read data from a delta table outside of the DLT pipeline by changing the schema to another catalog.schema

It is also possible to create resources using custom functions. User can define these new functions by following a pattern specified in the custom_function.py. This rerouces must be of type CUSTOM



- pass spark to functions
- can define a template to encapsulate the commmon fields. each child can override the fields as needed. this helps with defining the common fields ony once
- type and target_table_name field are required
- Can combine this with DABs to define environment dependent configs. These will be part of pipeline settings: e.g., evn: dev, source_databse: test_db, ...
- There are some predefined templates and types (append only, upsert, single sql, ...). Users can define their own functions in python which needs to be wrapped using ??? decorator. this will create a dlt table or views 
