"""
List the available extractors and categorize them.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import pkgutil
import importlib
import inspect

from graph import extractor
from graph.extractor.imcce_extractor import ImcceExtractor
from graph.extractor.nssdc_extractor import NssdcExtractor
from graph.extractor.wikidata_extractor import WikidataExtractor
from graph.extractor.n2yo_extractor import N2yoExtractor



class ExtractorLists():


    @staticmethod
    def _load_extractors():
        extractors = []

        for module_info in pkgutil.iter_modules(extractor.__path__):
            module_name = module_info.name

            module = importlib.import_module(f"graph.extractor.{module_name}")

            for name, obj in inspect.getmembers(module, inspect.isclass):
                if name.endswith("Extractor"):
                    extractors.append(obj)

        return extractors

    AVAILABLE_EXTRACTORS = _load_extractors.__func__()
    
    AVAILABLE_NAMESPACES = [e.NAMESPACE for e in AVAILABLE_EXTRACTORS]

    EXTRACTORS_BY_NAMES = {extractor.NAMESPACE: extractor for extractor in AVAILABLE_EXTRACTORS}

    NON_AUTHORITATIVE_EXTRACTORS = [ImcceExtractor,
                                    NssdcExtractor,
                                    WikidataExtractor,
                                    N2yoExtractor]

    AUTHORITATIVE_EXTRACTORS = set(AVAILABLE_EXTRACTORS) - set(NON_AUTHORITATIVE_EXTRACTORS)
