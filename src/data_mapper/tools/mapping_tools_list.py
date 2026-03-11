"""
List the available scorers and categorize them.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import pkgutil
import inspect
import importlib

from data_mapper.tools.filters.filter import Filter
from data_mapper.tools.matchers.matcher import Matcher
from data_mapper.tools.scorers.scorer import Scorer
from data_mapper.tools.embedders.embedder import Embedder
from data_mapper.tools import filters
from data_mapper.tools import matchers
from data_mapper.tools import scorers
from data_mapper.tools import embedders

class MappingToolsList():

    FILTERS = []
    for loader, module_name, is_pkg in pkgutil.iter_modules(filters.__path__):
        module = importlib.import_module(f"{filters.__name__}.{module_name}")
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, Filter) and obj is not Filter:
                    FILTERS.append(obj)

    MATCHERS = []
    for loader, module_name, is_pkg in pkgutil.iter_modules(matchers.__path__):
        module = importlib.import_module(f"{matchers.__name__}.{module_name}")
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, Matcher) and obj is not Matcher:
                    MATCHERS.append(obj)

    SCORER_TOOLS = []
    for loader, module_name, is_pkg in pkgutil.iter_modules(scorers.__path__):
        module = importlib.import_module(f"{scorers.__name__}.{module_name}")

        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, Scorer) and obj is not Scorer:
                    SCORER_TOOLS.append(obj)

    EMBEDDER_TOOLS = []
    for loader, module_name, is_pkg in pkgutil.iter_modules(embedders.__path__):
        module = importlib.import_module(f"{embedders.__name__}.{module_name}")

        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, Embedder) and obj is not Embedder:
                    EMBEDDER_TOOLS.append(obj)

    ALL_TOOLS = FILTERS + MATCHERS + SCORER_TOOLS + EMBEDDER_TOOLS
    TOOLS_BY_NAMES = {tool.NAME: tool for tool in ALL_TOOLS}