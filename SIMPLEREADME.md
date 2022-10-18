### Installation

pip install in your Databricks Notebook

```python
%pip install dlt_with_debug
```

### Example Usage

**Note**: You must define a `pipeline_id` variable as `spark.conf.get("pipelines.id", None)`

**Note**: You must define a `g` variable as `globals()`
`

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dlt_with_debug import *

pipeline_id =  spark.conf.get("pipelines.id", None)
g = globals()

if pipeline_id:
  import dlt

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"

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
df = clickstream_raw()
df.display()


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
df = clickstream_clean()
df.display()

```