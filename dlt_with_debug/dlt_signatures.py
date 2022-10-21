"""
This file contains the empty placeholder signatures of the dlt APIs
"""
from functools import wraps


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

def expect(name=None,
           inv=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_or_drop(name=None,
                   inv=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_or_fail(name=None,
                   inv=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_all(expectations=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_all_or_drop(expectations=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return true_decorator


def expect_all_or_fail(expectations=None):
    def true_decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return true_decorator
