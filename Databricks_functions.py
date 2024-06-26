import dlt
from pyspark.sql.functions import when, col, lit
from Databricks_templates import *



table_configurations = [] # this will contain all table configurations, can load different json files into this or use functions to add configs

def set_up_table(*args, **kwargs):
    config = kwargs["config"]
    
    target_table_name = config["target_table_name"]

    comment = config.get("comment", "")

    expect_all_criteria = config.get("expect_all_criteria", {})
    expect_all_or_drop_criteria = config.get("expect_all_or_drop", {})
    expect_all_or_fail_criteria = config.get("expect_all_or_fail", {})

    def wrapper(func):

      apply_expectations = len(expect_all_criteria)+ len(expect_all_or_drop_criteria)+len(expect_all_or_fail_criteria)

      if apply_expectations:
        @dlt.expect_all(expect_all_criteria)
        @dlt.expect_all_or_drop(expect_all_or_drop_criteria)
        @dlt.expect_all_or_fail(expect_all_or_fail_criteria)        
        @dlt.table(
                  name=f"{target_table_name}",
                  comment=f"{comment}",
                  table_properties={
                      "quality": "bronze"
                      }
              )  
        def inner():
            return func()
        return inner
      else:
        @dlt.table(
                  name=f"{target_table_name}",
                  comment=f"{comment}",
                  table_properties={
                      "quality": "bronze"
                      }
              )  
        def inner():
            return func()
        return inner

    return wrapper

# has to be a streaming table
def create_table_append_only(spark, config):

    source_table_name = config["source_table_name"]
    source_schema = config["source_schema"]
      
    @set_up_table(config=config)
    def create_table():
      if source_schema== "LIVE":
        df = dlt.read_stream(f"{source_table_name}")
      else:
        df = spark.readStream.table(f"{source_schema}.{source_table_name}")

      return df
        

# has to be a streaming table
def create_sql_table(spark, config):

    sql_query = config["sql_query"]
    args = config["args"]

    @set_up_table(config=config)
    def create_table():

      if len(args):
        # sql_str = "SELECT * FROM {source_table}".format(**args) 
        sql_str = sql_query.format(**args) 
        df = spark.sql(sql_str)
      else: 
        df = spark.sql(sql_query)

      return df
        


def upsert_into_table(spark, config):

  target_table_name = config["target_table_name"]
  comment = config["comment"]
  expect_all_criteria = config["expect_all_criteria"]
  expect_all_or_drop_criteria = config["expect_all_or_drop"]
  expect_all_or_fail_criteria = config["expect_all_or_fail"]    

  source_table_name = config["source_table_name"]
  source_schema = config["source_schema"]


  dlt.create_streaming_table(
      name=target_table_name,
      expect_all=expect_all_criteria,
      expect_all_or_drop=expect_all_or_drop_criteria,
      expect_all_or_fail=expect_all_or_fail_criteria,
  )

 
  if source_schema== "LIVE":
    dlt.apply_changes(
      target = target_table_name,
      source = f"{source_table_name}",
      keys = ["id"],
      sequence_by = col("ingesttime"),
      stored_as_scd_type = "2"
    )
  else:
      dlt.apply_changes(
      target = target_table_name,
      source = f"{source_schema}.{source_table_name}",
      keys = ["id"],
      sequence_by = col("ingesttime"),
      stored_as_scd_type = "2"
    )
      
def process_parent_template(config):
      #add fields from parent template that are not overwrriten to each table config
    #---------------------------------
    if "parent_template" in config.keys():
      parent = config["parent_template"].copy() ### this is important otherwise we'll modify the original append_only_template 
      keys = list(config.keys())
      for key in keys:
          parent.pop(key, None)
      config.update(parent)
      config.pop("parent_template", None)
    #---------------------------------

    return config


# for append only tables target_table_name and source_table_name are required the resst is optional 
def add_append_only_table(target_table_name, source_table_name, **kwargs):

  comment = kwargs.get("comment", "")
  # or can write a for loop to add all the args to the config  

  item_config = {
  "parent_template": append_only_template,
  "target_table_name": target_table_name,
  "source_table_name": source_table_name,
  "comment": comment
  }
  table_configurations.append(item_config)



