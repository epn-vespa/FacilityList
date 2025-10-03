"""
Versioning annotator for the project's functions.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""

def deprecated(func):
    def wrapper(*args, **kwargs):
        if hasattr(func, '__self__'):
            class_name = func.__self__.__class__.__name__
            func_name = func.__name__
            key = f"{class_name}.{func_name}"
        else:
            module_name = func.__module__
            func_name = func.__name__
            key = f"{module_name}.{func_name}"
        print(f"Warning: {key}() is deprecated.")
        result = func(*args, **kwargs)
        return result
    return wrapper