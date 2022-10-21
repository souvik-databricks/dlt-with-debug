from inspect import isfunction, ismethod, isclass


def remove_dltwithdebug_decorator(code):
    parsed_code = code.split("\n")
    parsed_code = [i if (i.startswith('@dlt.table(') == False) else i.replace('@dlt.table(','@dlt.create_table(') for i in parsed_code ]
    parsed_code = [i for i in parsed_code if i.startswith('@dltwithdebug') == False]
    parsed_code = '\n'.join(parsed_code)
    return parsed_code


def check_if_decorator(a):
    return isfunction(a) or ismethod(a) or isclass(a)


def undecorated(o):
    """Unpack all decorators from a function, method or class"""
    if type(o) is type:
        return o
    try:
        closure = o.__closure__
    except AttributeError:
        return
    if closure:
        for cell in closure:
            if cell.cell_contents is o:
                continue

            if check_if_decorator(cell.cell_contents):
                undecd = undecorated(cell.cell_contents)
                if undecd:
                    return undecd
        else:
            return o
    else:
        return o
