import time


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