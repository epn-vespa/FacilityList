"""
List the available extractors and categorize them.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""


from graph.extractor.aas_extractor import AasExtractor
from graph.extractor.iaumpc_extractor import IauMpcExtractor
from graph.extractor.imcce_extractor import ImcceExtractor
from graph.extractor.naif_extractor import NaifExtractor
from graph.extractor.nssdc_extractor import NssdcExtractor
from graph.extractor.pds_extractor import PdsExtractor
from graph.extractor.spase_extractor import SpaseExtractor
from graph.extractor.wikidata_extractor import WikidataExtractor



class ExtractorLists():

    AVAILABLE_EXTRACTORS = [AasExtractor,
                            IauMpcExtractor,
                            ImcceExtractor,
                            NaifExtractor,
                            NssdcExtractor,
                            PdsExtractor,
                            SpaseExtractor,
                            WikidataExtractor]
    
    AVAILABLE_NAMESPACES = [e.NAMESPACE for e in AVAILABLE_EXTRACTORS]

    EXTRACTORS_BY_NAMES = {extractor.NAMESPACE: extractor for extractor in AVAILABLE_EXTRACTORS}

    NON_AUTHORITATIVE_EXTRACTORS = [ImcceExtractor,
                                    NssdcExtractor,
                                    WikidataExtractor]

    AUTHORITATIVE_EXTRACTORS = set(AVAILABLE_EXTRACTORS) - set(NON_AUTHORITATIVE_EXTRACTORS)
