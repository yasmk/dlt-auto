from Databricks_functions import set_up_table

#sample custom defined function
def my_custom_function(spark, config):
   
    @set_up_table(config=config)
    def create_table_df():

      df1 = dlt.read(config["source_table_names"][0])

      df2 = spark.read.table(config["source_table_names"][1])

      joined_df = df1.join(df2, on='id', how='inner').select(df1.id, df1.value)

      return joined_df
    

