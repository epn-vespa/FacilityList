import time
import atexit
from collections import defaultdict
from multiprocessing import Manager

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

manager = Manager()
times = manager.dict() # defaultdict(float)

def timeall(func):
    """
    Time the total time spent on the call of a method.
    Print it in the end.
    """
    def wrapper(*args, **kwargs):
        if hasattr(func, '__self__'):  # Si c'est une méthode d'une classe
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
        if key not in times:
            times[key] = 0
        times[key] += elapsed

        return result
    return wrapper

def printtimes():
    global times
    if times:
        # Sort on times
        times = sorted(times.items(), key = lambda x: x[1])
        for func_name, time in times:
            print(f"{func_name}\t\t{time}")

atexit.register(printtimes)


def deprecated(func):
    def wrapper(*args, **kwargs):
        if hasattr(func, '__self__'):  # Si c'est une méthode d'une classe
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