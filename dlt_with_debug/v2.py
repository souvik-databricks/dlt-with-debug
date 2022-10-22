from functools import wraps
from inspect import getsource
from dlt_with_debug.helpers import undecorated, remove_dltwithdebug_decorator
from dlt_with_debug.dlt_signatures import addglobals
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
pipeline_id = spark.conf.get("pipelines.id", None)


def dltwithdebug(g_ns):
  def true_decorator(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
      if pipeline_id:
        return f(*args, **kwargs)
      else:
        f_undec = undecorated(f)
        code = getsource(f_undec)
        parsed_code = remove_dltwithdebug_decorator(code)
        addglobals(g_ns)
        exec(parsed_code, g_ns)
        return f(*args, **kwargs)
    return wrapped
  return true_decorator

def showoutput(f):
  if not pipeline_id:
    df = f()
    df.display()
  else:
    None
