# Databricks notebook source
# MAGIC %pip install dlt-with-debug

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

from dlt_with_debug import dltwithdebug, pipeline_id, showoutput

if pipeline_id:
  import dlt
else:
  from dlt_with_debug import dlt

# COMMAND ----------

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"

# COMMAND ----------

@dlt.create_table(
  comment="The raw wikipedia click stream dataset, ingested from /databricks-datasets.",
  table_properties={
    "quality": "bronze"
  }
)
@dltwithdebug(globals())
def clickstream_raw():
  return (
    spark.read.option("inferSchema", "true").json(json_path)
  )

# COMMAND ----------

showoutput(clickstream_raw)

# COMMAND ----------

@dlt.create_table(
  comment="Wikipedia clickstream dataset with cleaned-up datatypes / column names and quality expectations.",
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect("valid_current_page", "current_page_id IS NOT NULL AND current_page_title IS NOT NULL")
@dlt.expect_or_fail("valid_count", "click_count > 0")
@dlt.expect_all({'valid_prev_page_id': "previous_page_id IS NOT NULL"})
@dltwithdebug(globals())
def clickstream_clean():
  return (
    dlt.read("clickstream_raw")
      .withColumn("current_page_id", expr("CAST(curr_id AS INT)"))
      .withColumn("click_count", expr("CAST(n AS INT)"))
      .withColumn("previous_page_id", expr("CAST(prev_id AS INT)"))
      .withColumnRenamed("curr_title", "current_page_title")
      .withColumnRenamed("prev_title", "previous_page_title")
      .select("current_page_id", "current_page_title", "click_count", "previous_page_id", "previous_page_title")
  )

# COMMAND ----------

showoutput(clickstream_clean)

# COMMAND ----------


