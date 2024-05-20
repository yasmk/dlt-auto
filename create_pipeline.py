# Databricks notebook source
import dlt
import sys
import os

# sys.path.append(os.path.abspath('<module-path>'))

from Databricks_templates import *
from Databricks_functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Wrapper to config table (e.g., add expectations if they're defined)

# COMMAND ----------

# def set_up_table(*args, **kwargs):
#     config = kwargs["config"]
#     target_table_name = config["target_table_name"]
#     comment = config["comment"]
#     expect_all_criteria = config["expect_all_criteria"]
#     expect_all_or_drop_criteria = config["expect_all_or_drop"]
#     expect_all_or_fail_criteria = config["expect_all_or_fail"]    

#     def wrapper(func):

#       apply_expectations = len(config["expect_all_criteria"])+ len(config["expect_all_or_drop"])+len(config["expect_all_or_fail"])

#       if apply_expectations:
#         @dlt.expect_all(expect_all_criteria)
#         @dlt.expect_all_or_drop(expect_all_or_drop_criteria)
#         @dlt.expect_all_or_fail(expect_all_or_fail_criteria)        
#         @dlt.table(
#                   name=f"{target_table_name}",
#                   comment=f"{comment}",
#                   table_properties={
#                       "quality": "bronze"
#                       }
#               )  
#         def inner():
#             return func()
#         return inner
#       else:
#         @dlt.table(
#                   name=f"{target_table_name}",
#                   comment=f"{comment}",
#                   table_properties={
#                       "quality": "bronze"
#                       }
#               )  
#         def inner():
#             return func()
#         return inner

#     return wrapper


# COMMAND ----------

# MAGIC %md
# MAGIC ##Functions for the actuall transforms (example readstream from source)
# MAGIC - Can be apply changes or a custom function
# MAGIC - Can send sql queries to the fucntion

# COMMAND ----------

# # has to be a streaming table
# def create_table_append_only(config):

#     source_table_name = config["source_table_name"]
#     source_schema = config["source_schema"]
      
#     @set_up_table(config=config)
#     def create_bronze_table_df():
#       if source_schema== "LIVE":
#         df = dlt.read_stream(f"{source_table_name}")
#       else:
#         df = spark.readStream.table(f"{source_schema}.{source_table_name}")

#       return df
        


# COMMAND ----------

# from pyspark.sql.functions import when, col, lit

# def upsert_into_table(config):

#   target_table_name = config["target_table_name"]
#   comment = config["comment"]
#   expect_all_criteria = config["expect_all_criteria"]
#   expect_all_or_drop_criteria = config["expect_all_or_drop"]
#   expect_all_or_fail_criteria = config["expect_all_or_fail"]    

#   source_table_name = config["source_table_name"]
#   source_schema = config["source_schema"]


#   dlt.create_streaming_table(
#       name=target_table_name,
#       expect_all=expect_all_criteria,
#       expect_all_or_drop=expect_all_or_drop_criteria,
#       expect_all_or_fail=expect_all_or_fail_criteria,
#   )

#   # dlt.create_streaming_table(
#   #     name=target_table_name,
#   #     table_properties=tableProperties,
#   #     partition_cols=partitionColumns,
#   #     path=target_path,
#   #     schema=struct_schema,
#   #     expect_all=expect_all_dict,
#   #     expect_all_or_drop=expect_all_or_drop_dict,
#   #     expect_all_or_fail=expect_all_or_fail_dict,
#   # )


#   if source_schema== "LIVE":
#     dlt.apply_changes(
#       target = target_table_name,
#       source = f"{source_table_name}",
#       keys = ["id"],
#       sequence_by = col("ingesttime"),
#       stored_as_scd_type = "2"
#     )
#   else:
#       dlt.apply_changes(
#       target = target_table_name,
#       source = f"{source_schema}.{source_table_name}",
#       keys = ["id"],
#       sequence_by = col("ingesttime"),
#       stored_as_scd_type = "2"
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##configurations and the loop to create the tables

# COMMAND ----------

# # fields that won't change often for differnt tables can go here
# #----------------------------------------
# append_only_template = {
#   "type": "append_only",
#   "target_schema": "LIVE",
#   "source_schema": "LIVE",
#   "expect_all_criteria" : {},
#   "expect_all_or_drop": {},
#   "expect_all_or_fail": {}

# }

# upsert_template = {
#   "type": "apply_changes",
#   "target_schema": "LIVE",
#   "source_schema": "LIVE",
#   "expect_all_criteria" : {},
#   "expect_all_or_drop": {},
#   "expect_all_or_fail": {}
# }
# #----------------------------------------


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
        
