import dlt
from pyspark.sql.functions import when, col, lit


def set_up_table(*args, **kwargs):
    config = kwargs["config"]
    target_table_name = config["target_table_name"]
    comment = config["comment"]
    expect_all_criteria = config["expect_all_criteria"]
    expect_all_or_drop_criteria = config["expect_all_or_drop"]
    expect_all_or_fail_criteria = config["expect_all_or_fail"]    

    def wrapper(func):

      apply_expectations = len(config["expect_all_criteria"])+ len(config["expect_all_or_drop"])+len(config["expect_all_or_fail"])

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
    def create_bronze_table_df():
      if source_schema== "LIVE":
        df = dlt.read_stream(f"{source_table_name}")
      else:
        df = spark.readStream.table(f"{source_schema}.{source_table_name}")

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

  # dlt.create_streaming_table(
  #     name=target_table_name,
  #     table_properties=tableProperties,
  #     partition_cols=partitionColumns,
  #     path=target_path,
  #     schema=struct_schema,
  #     expect_all=expect_all_dict,
  #     expect_all_or_drop=expect_all_or_drop_dict,
  #     expect_all_or_fail=expect_all_or_fail_dict,
  # )


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
