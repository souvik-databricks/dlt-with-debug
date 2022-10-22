<div id="top"></div>
<br />
<div align="center">
  <img src="https://raw.githubusercontent.com/souvik-databricks/random_snaps/main/dlt_logo.png" alt="Delta Live Table" width="450" height="250">
  <h3 align="center">DLT with Debug</h3>

  <p align="center">
    Running DLT workflows from interactive notebooks.
    <br />
    <br />
  </p>
</div>

## About The Project

Delta Live Tables (DLTs) are a great way to design data pipelines with only focusing on the core business logic.
It makes the life of data engineers easy but while the development workflows are streamlined in DLT, when it comes to 
__*debugging and seeing how the data looks after each transformation step*__ in a typical DLT pipeline it becomes very 
painful and cumbersome as we dont have the DLT package available in our interactive environment.

Enter **dlt-with-debug** a lightweight decorator utility which allows developers to do interactive
pipeline development by having a unified source code for both DLT run and Non-DLT interactive notebook run. 


<p align="right">(<a href="#top">back to top</a>)</p>

### Built With

- Python's builtins
  - [globals()](https://docs.python.org/3/library/functions.html#globals)
  - [exec()](https://docs.python.org/3/library/functions.html#exec)
  - [decorator()](https://docs.python.org/3/glossary.html#term-decorator)

<p align="right">(<a href="#top">back to top</a>)</p>

### Installation

pip install in your Databricks Notebook

_**PyPI**_
```python
%pip install dlt-with-debug
```

<p align="right">(<a href="#top">back to top</a>)</p>


### Prerequisites

- [Databricks](https://databricks.com/) 
- [Delta Live Tables](https://databricks.com/product/delta-live-tables)

<p align="right">(<a href="#top">back to top</a>)</p>

### Usage
- *In our notebooks containing DLT Jobs the imports changes slightly as below*
    ```
    from dlt_with_debug import dltwithdebug, pipeline_id, showoutput
    
    if pipeline_id:
      import dlt
    else:
      from dlt_with_debug import dlt
    ```
> **Note**: 
> 1. Use the `dlt.create_table()` API instead of `dlt.table()` as `dlt.table()` sometimes gets mixed with `spark.table()` 
in the global namespace.
> 2. Always pass the `globals()` namespace to `dltwithdebug` decorator like this `@dltwithdebug(globals())`


<p align="right">(<a href="#top">back to top</a>)</p>

---

### Sample `DLT with debug` DLT pipeline example

> **Code**:

Cmd 1
```python
%pip install -e git+https://github.com/souvik-databricks/dlt-with-debug.git#"egg=dlt_with_debug"
```
Cmd 2
```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# We are importing 
# dltwithdebug as that's the entry point to interactive DLT workflows
# pipeline_id to ensure we import the dlt package based on environment
# showoutput is a helper function for seeing the output result along with expectation metrics if any is specified
from dlt_with_debug import dltwithdebug, pipeline_id, showoutput

if pipeline_id:
  import dlt
else:
  from dlt_with_debug import dlt
```
Cmd 3
```python
json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"
```
Cmd 4
```python
# Notice we are using dlt.create_table instead of dlt.table

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
```
Cmd 5
```python
# for displaying the result of the transformation 
# use showoutput(func_name)
# for example here we are using showoutput(clickstream_raw) 
showoutput(clickstream_raw)
```
![Alt Text](https://raw.githubusercontent.com/souvik-databricks/random_snaps/main/clck_raw.png)

Cmd 6
```python
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
```
Cmd 7
```python
showoutput(clickstream_clean)
```
![Alt Text](https://raw.githubusercontent.com/souvik-databricks/random_snaps/main/clck_clean.png)


---
> _Important to note that here you can see we are also **seeing how many records will the expectations affect**._
---
<p align="right">(<a href="#top">back to top</a>)</p>

## Same sample `DLT with debug` DLT pipeline executed as part of a delta live table
![Alt Text](https://i.ibb.co/VQzZsZR/Screenshot-2022-10-18-at-5-34-14-AM.png)

> Below we can the expectation results also match up with the expectation metrics that we got from dltwithdebug earlier 
> with `showoutput(clickstream_clean)`
> ![Expectation Results](https://raw.githubusercontent.com/souvik-databricks/random_snaps/main/expectations.png)

<p align="right">(<a href="#top">back to top</a>)</p>

## Quick API guide

#### Table syntax

```python
@dlt.create_table(   # <-- Notice we are using the dlt.create_table() instead of dlt.table()
  name="<name>",
  comment="<comment>",
  spark_conf={"<key>" : "<value", "<key" : "<value>"},
  table_properties={"<key>" : "<value>", "<key>" : "<value>"},
  path="<storage-location-path>",
  partition_cols=["<partition-column>", "<partition-column>"],
  schema="schema-definition",
  temporary=False)
@dlt.expect
@dlt.expect_or_fail
@dlt.expect_or_drop
@dlt.expect_all
@dlt.expect_all_or_drop
@dlt.expect_all_or_fail
@dltwithdebug(globals())    # <-- This dltwithdebug(globals()) needs to be added
def <function-name>():
    return (<query>)
```

#### View syntax

```python
@dlt.create_view(    # <-- Notice we are using the dlt.create_view() instead of dlt.view()
  name="<name>",
  comment="<comment>")
@dlt.expect
@dlt.expect_or_fail
@dlt.expect_or_drop
@dlt.expect_all
@dlt.expect_all_or_drop
@dlt.expect_all_or_fail
@dltwithdebug(globals())    # <-- This dltwithdebug(globals()) needs to be added
def <function-name>():
    return (<query>)
```

#### Getting results syntax

```python
showoutput(function_name)  # <-- showoutput(function_name) 
                           # Notice we are only passing the function name
                           # The name of the function which is wrapped by the dltdecorators
                           
                           # For example:
                           # @dlt.create_table()
                           # @dltwithdebug(globals())
                           # def step_one():
                           #    return spark.read.csv()

                           # showoutput(step_one)
```

#### Import syntax

```python
# We are importing 
# dltwithdebug as that's the entry point to interactive DLT workflows
# pipeline_id to ensure we import the dlt package based on environment
# showoutput is a helper function for seeing the output result along with expectation metrics if any is specified
from dlt_with_debug import dltwithdebug, pipeline_id, showoutput

if pipeline_id:
  import dlt
else:
  from dlt_with_debug import dlt
```

<p align="right">(<a href="#top">back to top</a>)</p>

## Upcoming functionality

As of now the following DLT APIs are covered for interactive use:

1. **Currently Available:**

   - `dlt.read`
   - `dlt.read_stream`
   - `dlt.create_table`
   - `dlt.create_view`
   - `dlt.table` <-- This one sometimes gets overridden with `spark.table`
   - `dlt.view` 
   - `dlt.expect`
   - `dlt.expect_or_fail`
   - `dlt.expect_or_drop`
   - `dlt.expect_all`
   - `dlt.expect_all_or_drop`
   - `dlt.expect_all_or_fail`

2. **Will be covered in the upcoming release:**
   - `dlt.create_target_table`
   - `dlt.create_streaming_live_table`
   - `dlt.apply_changes`

## Limitation

`DLT with Debug` is a fully python based utility and as such it doesn't supports `spark.table("LIVE.func_name")` syntax. 

So instead of `spark.table("LIVE.func_name")` use `dlt.read("func_name")` or `dlt.read_stream("func_name")`

## License

Distributed under the MIT License.

<p align="right">(<a href="#top">back to top</a>)</p>