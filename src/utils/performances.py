import atexit
import time
from collections import defaultdict
from functools import wraps
from weakref import WeakKeyDictionary



def timeit(func):
    def wrapper(*args, **kwargs):
        print(f"Starting to run {func.__name__}().")
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        elapsed = end - start
        print(f"\t{func.__name__}() finished after: {elapsed:.6f} seconds.")
        return result
    return wrapper


times = defaultdict(int)

def timeall(func):
    """
    Time the total time spent on the call of a method.
    Print it in the end.
    """
    global times
    def wrapper(*args, **kwargs):
        if hasattr(func, '__self__'):
            class_name = func.__self__.__class__.__name__
            func_name = func.__name__
            key = f"{class_name}.{func_name}"
        else:
            module_name = func.__module__
            func_name = func.__name__
            key = f"{module_name}.{func_name}"

        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        elapsed = end - start
        times[key] += elapsed

        return result
    return wrapper


def print_execution_report():
    global times
    if times:
        # Sort on times
        times = sorted(times.items(), key = lambda x: x[1])
        for func_name, time in times:
            print(f"{func_name}\t\t{time}")

atexit.register(print_execution_report)


def make_hashable(obj):
    """
    Recursively transform lists, dicts & sets into tuples in an object/

    Args:
        obj: an object of any type
    Returns:
        the object converted with only tuples and frozenset
    """
    if isinstance(obj, list):
        return tuple(make_hashable(x) for x in obj)

    if isinstance(obj, dict):
        return tuple(
            sorted(
                (make_hashable(k), make_hashable(v))
                for k, v in obj.items()
            )
        )

    if isinstance(obj, tuple):
        return tuple(make_hashable(x) for x in obj)

    if isinstance(obj, set):
        return frozenset(make_hashable(x) for x in obj)

    return obj


class CachedMethod:
    """
    Wrapper to set on object methods to accelerate them if they
    are called a lot of times with the exact same arguments.

    Similar to functools.cache, but allows to remove the whole cache
    and is dependant on an instance.
    """
    def __init__(self, func):
        self.func = func
        # self.cache_name = f"_cache_{func.__name__}"
        self.cache = WeakKeyDictionary()

    def __get__(self, instance, owner):
        if instance is None:
            return self

        @wraps(self.func)
        def wrapper(*args: tuple,
                    **kwargs: dict):
            """
            cache = instance.__dict__.setdefault(
                "_method_cache",
                {}
            )"""
            instance_cache = self.cache.setdefault(instance, {})
            # method_cache = cache.setdefault(self.name, {})
            # tuples can be keys of dicts.
            key = make_hashable((args, kwargs))
            if key not in instance_cache:
                instance_cache[key] = self.func(instance, *args, **kwargs)
            return instance_cache[key]
        return wrapper


    def clear(self, instance):
        """
        Clear the cache (to trigger when the arguments point to objects
        that will change the output of the cached method)
        """
        self.cache.pop(instance, None)
