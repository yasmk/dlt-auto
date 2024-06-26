# Databricks notebook source
import dlt
import sys
import os

# sys.path.append(os.path.abspath('<module-path>'))

from Databricks_templates import *
from Databricks_functions import *
from custom_functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##configurations

# COMMAND ----------

def call_custom_function(function_name, spark, config):
      func_to_call = globals().get(function_name)
      # if func_to_call:
      func_to_call(spark, config)


# COMMAND ----------

# source_database specified in the pipeline config (Can be changed based on dev, prod, ..., by DABs) 
source_database = spark.conf.get("mypipeline.source_database")

# each table can "inherit" a parent template 
#-----------------------------------------
json_configuration = [
  # append only table no expectations, source another LIVE table
  {
  "parent_template": append_only_template,
  "target_table_name": "bronze_table_1",
  "source_table_name": "streaming_source",
  "comment": "Bronze table from live data source",
},
# append only table no expectations, source another LIVE table
                     {
  "parent_template": append_only_template,                       
  "target_table_name": "bronze_table_2",
  "source_table_name": "streaming_source",
  "comment": "Bronze table from live data source",
  "expect_all_or_drop": {"valid_value_streaming": "value < 50"},
},
# append only table no expectations, source table from source_database specified in the pipeline config (Can be changed based on dev, prod, ...)                                          
                     {
  "parent_template": append_only_template,                       
  "target_table_name": "bronze_table_3",
  "source_table_name": "mv_source_1",
  "source_schema": source_database,
  "comment": "Bronze table from external (e.g., delta share) source"
},
                     
                                          {
  "parent_template": upsert_template,                       
  "target_table_name": "silver_table_1",
  "source_table_name": "bronze_table_2",
  "comment": "Silver table SCD"
},
#fixed SQL query
{
  "parent_template": sql_template,
  "target_table_name": "silver_table_2",
  "sql_query": "SELECT * FROM live.bronze_table_2",
  "comment": "Silver table mv"
}                 
,
#parameterised SQL query
{
  "parent_template": sql_template,
  "target_table_name": "silver_table_3",
  "sql_query": "SELECT * from {source_Table_1}",
  "args": { "source_Table_1" : "live.bronze_table_2"},
  "comment": "Silver table mv sql and args"
},
# sample custom user function
{
  "type": "CUSTOM",
  "target_table_name": "silver_table_4",
  "source_table_names": ["bronze_table_1", f"{source_database}.mv_source_1"],
  "function" : "my_custom_function"
}                                          
            ]
#-----------------------------------------



# COMMAND ----------

#json configuration can be loaded from multiple json files and based on the path defnied using DABs
table_configurations.extend(json_configuration)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Another method to add config items: Use such functions to add the config. this might be easier than writing json, can take care of default values and do some vaidation on the config

# COMMAND ----------

add_append_only_table(target_table_name="function_added_bronze_table_5", source_table_name="streaming_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create  tables based on config

# COMMAND ----------


for config in table_configurations: 

    config = process_parent_template(config)
    
    if config["type"] == "append_only":
        create_table_append_only(spark, config)

    if config["type"] == "apply_changes":
        upsert_into_table(spark, config)

    if config["type"] == "sql_table":
        create_sql_table(spark, config)

    if config["type"] == "CUSTOM":
      call_custom_function(config["function"], spark, config)
        
