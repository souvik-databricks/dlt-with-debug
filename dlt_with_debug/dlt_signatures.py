"""
This file contains the empty placeholder signatures of the dlt APIs
"""
from functools import wraps
from dlt_with_debug.helpers import undecorated
import builtins as orig

g_ns_for_placeholders = globals()
addglobals = lambda x: g_ns_for_placeholders.update(x)

def read(arg):
    return g_ns_for_placeholders[arg]()


def read_stream(arg):
    return g_ns_for_placeholders[arg]()


def table(name=None,
          comment=None,
          spark_conf=None,
          table_properties=None,
          path=None,
          partition_cols=None,
          schema=None,
          temporary=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return true_decorator

create_table = table


def view(name=None,
         comment=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return true_decorator

create_view = view

def get_name_inv_statement(f,name,inv):
    func = undecorated(f)
    count = func().filter(inv).count()
    total = func().count()
    stmt = f"Expectation `{name}` will affect {total-count} records which is {orig.round(((total-count)/total)*100,2)}% of total {total} records"
    return stmt


def expect(name=None,
           inv=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if name:
                stmt = "'expect' "+get_name_inv_statement(f,name,inv)
                print(stmt)
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_or_drop(name=None,
                   inv=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if name:
                stmt = "'expect_or_drop' "+get_name_inv_statement(f,name,inv)
                print(stmt)
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_or_fail(name=None,
                   inv=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if name:
                stmt = "'expect_or_fail' "+get_name_inv_statement(f,name,inv)
                print(stmt)
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def get_expectations_statement(f,expectations):
    func = undecorated(f)
    expec_lst = list(expectations.values())
    expec_lst = ["(" + str(i) + ")" for i in expec_lst]
    expec_cond = " AND ".join(expec_lst)
    count = func().filter(expec_cond).count()
    total = func().count()
    expec_txt = " AND ".join(list(expectations.keys()))
    stmt = f"Expectations `{expec_txt}` will affect {total-count} records which is {orig.round(((total-count) / total) * 100, 2)}% of total {total} records"
    return stmt


def expect_all(expectations=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if expectations:
                stmt = "'expect_all' "+get_expectations_statement(f,expectations)
                print(stmt)
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_all_or_drop(expectations=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if expectations:
                stmt = "'expect_all_or_drop' "+get_expectations_statement(f,expectations)
                print(stmt)
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_all_or_fail(expectations=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if expectations:
                stmt = "'expect_all_or_fail' "+get_expectations_statement(f,expectations)
                print(stmt)
            return f(*args, **kwargs)

        return wrapped

    return true_decorator
