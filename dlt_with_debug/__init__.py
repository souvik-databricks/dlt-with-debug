from dlt_with_debug.main import dltwithdebug

print("""
Ensure that the below two lines are added in the beginning of the notebook

pipeline_id =  spark.conf.get("pipelines.id", None)
g = globals()
""")
