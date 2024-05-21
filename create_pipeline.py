# Databricks notebook source
import dlt
import sys
import os

# sys.path.append(os.path.abspath('<module-path>'))

from Databricks_templates import *
from Databricks_functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##configurations and the loop to create the tables

# COMMAND ----------

# source_database specified in the pipeline config (Can be changed based on dev, prod, ..., by DABs?) 
source_database = spark.conf.get("mypipeline.source_database")

# each table can "inherit" a parent template 
#-----------------------------------------
table_configurations = [
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

{
  "parent_template": sql_template,
  "target_table_name": "silver_table_2",
  "sql_query": "SELECT * FROM live.bronze_table_2",
  "comment": "Silver table mv"
}                 
,
{
  "parent_template": sql_template,
  "target_table_name": "silver_table_3",
  "sql_query": "SELECT * from {source_Table_1}",
  "args": { "source_Table_1" : "live.bronze_table_2"},
  "comment": "Silver table mv sql and args"
}                 
                                          
            ]
#-----------------------------------------


for config in table_configurations: 

    #add fields from parent template that are not overwrriten to each table config
    #---------------------------------
    parent = config["parent_template"].copy() ### this is important otherwise we'll modify the original append_only_template 
    keys = list(config.keys())
    for key in keys:
        parent.pop(key, None)
    config.update(parent)
    config.pop("parent_template", None)
    #---------------------------------
    
    if config["type"] == "append_only":
        create_table_append_only(spark, config)

    if config["type"] == "apply_changes":
        upsert_into_table(spark, config)

    if config["type"] == "sql_table":
        create_sql_table(spark, config)

        
        
