# Databricks notebook source
import dlt

from pyspark.sql.functions import when, col, lit
from pyspark.sql.functions import current_timestamp, rand, round
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
    StringType,
)


@dlt.table()
def streaming_source():
    rows_per_second = 10

    df = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", str(rows_per_second))
        .load()
        .withColumn("ingesttime", current_timestamp())
        .withColumn("id", (rand() * 10000 + 1).cast("integer")).withColumn("value", (rand() * 10 + 1).cast("integer"))
    )

    return df


@dlt.table()
def mv_source():

    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("value", IntegerType()),
            StructField("ingesttime", StringType()),
        ]
    )

    df = spark.createDataFrame(
        data=[
            {"id": 1, "value": 20, "ingesttime": str(current_timestamp())},
            {"id": 2, "value": 300, "ingesttime": str(current_timestamp())},
        ],
        schema=schema,
    )

    return df

