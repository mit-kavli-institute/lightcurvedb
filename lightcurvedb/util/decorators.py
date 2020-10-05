from functools import wraps

def cast_to(type_):
    def external_wrap(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            return type_(func)
        return wrapper
    return external_wrap
