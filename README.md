<div id="top"></div>
<br />
<div align="center">

  <h3 align="center">DLT with Debug</h3>

  <p align="center">
    Running DLT workflows from interactive notebooks.
    <br />
    <br />
  </p>
</div>

## About The Project

Delta Live Tables (DLTs) are a great way to design data pipelines with only focusing on the core business logic.
Makes the life of data engineers easy but while the development workflows are streamlined in DLT, when it comes to 
__*debugging and seeing how the data looks after each transformation step*__ in a typical DLT pipeline it becomes very 
painful and cumbersome. Since we dont have DLT available in our interactive notebooks.

Enter <font size="4"><b>dlt with debug</b></font> a lightweight helper utility which allows developers to do interactive
pipeline development by having a unified source code for both DLT run and Non-DLT interactive notebook run. 


<p align="right">(<a href="#top">back to top</a>)</p>

### Built With

- Python's builtins
  - [globals()](https://docs.python.org/3/library/functions.html#globals)
  - [exec()](https://docs.python.org/3/library/functions.html#exec)

<p align="right">(<a href="#top">back to top</a>)</p>


### Prerequisites

- [Databricks](https://databricks.com/) 
- [Delta Live Tables](https://databricks.com/product/delta-live-tables)
- *in our DLT Jobs two lines needs to be added in the beginning as a must*
    ```
    pipeline_id =  spark.conf.get("pipelines.id", None)
    g = globals()
    ```

<p align="right">(<a href="#top">back to top</a>)</p>


### Installation

pip install in your Databricks Notebook

```python
%pip install dlt_with_debug
```

<p align="right">(<a href="#top">back to top</a>)</p>

### Sample `DLT with debug` Job code

> **Note**:

1. You must define a `pipeline_id` variable as `spark.conf.get("pipelines.id", None)`

2. You must define a `g` variable as `globals()`

> **Code**:

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

if not pipeline_id:
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

if not pipeline_id:
  df = clickstream_clean()
  df.display()

```

![Alt Text](https://i.ibb.co/VQzZsZR/Screenshot-2022-10-18-at-5-34-14-AM.png)

<p align="right">(<a href="#top">back to top</a>)</p>

## License

Distributed under the MIT License.

<p align="right">(<a href="#top">back to top</a>)</p>