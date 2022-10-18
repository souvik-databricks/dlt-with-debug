# Databricks notebook source
# MAGIC %pip install dlt_with_debug

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from dlt_with_debug import *

# COMMAND ----------

pipeline_id = spark.conf.get("pipelines.id", None)
g = globals()

# COMMAND ----------

if pipeline_id:
    import dlt

# COMMAND ----------

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"

# COMMAND ----------

step = """
@dlt.create_table(
  comment="The raw wikipedia click stream dataset, ingested from /databricks-datasets.",
  table_properties={
    "quality": "bronze"
  }
)
def clickstream_raw():
  return (
    spark.read.option("inferSchema", "true").json(json_path)
  )
"""
dltwithdebug(step, pipeline_id, g)

if not pipeline_id:
    df = clickstream_raw()
    df.display()

# COMMAND ----------

step = """
@dlt.create_table(
  comment="Wikipedia clickstream dataset with cleaned-up datatypes / column names and quality expectations.",
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect("valid_current_page", "current_page_id IS NOT NULL AND current_page_title IS NOT NULL")
@dlt.expect_or_fail("valid_count", "click_count > 0")
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
"""
dltwithdebug(step, pipeline_id, g)

if not pipeline_id:
    df = clickstream_clean()
    df.display()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## The job linked [here](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/08c3ccd3-15b3-450a-9860-fb4482d1d9b8/updates/57549f72-7f39-4fa5-bf87-24e4d6504e91) 
# MAGIC 
# MAGIC <a href="https://ibb.co/Yy1MxMm"><img src="https://i.ibb.co/MGt4m4r/Screenshot-2022-10-18-at-5-34-14-AM.png" alt="Screenshot-2022-10-18-at-5-34-14-AM" border="0"></a>

# COMMAND ----------


