"""
List the available extractors and categorize them.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""


from data_updater.extractor.aas_extractor import AasExtractor
from data_updater.extractor.iaumpc_extractor import IauMpcExtractor
from data_updater.extractor.imcce_extractor import ImcceExtractor
from data_updater.extractor.naif_extractor import NaifExtractor
from data_updater.extractor.nssdc_extractor import NssdcExtractor
from data_updater.extractor.pds_extractor import PdsExtractor
from data_updater.extractor.spase_extractor import SpaseExtractor
from data_updater.extractor.wikidata_extractor import WikidataExtractor


class ExtractorLists():

    AVAILABLE_EXTRACTORS = [AasExtractor,
                            IauMpcExtractor,
                            ImcceExtractor,
                            NaifExtractor,
                            NssdcExtractor,
                            PdsExtractor,
                            SpaseExtractor,
                            WikidataExtractor]

    EXTRACTORS_BY_NAMES = {extractor.NAMESPACE: extractor for extractor in AVAILABLE_EXTRACTORS}

    NON_AUTHORITATIVE_EXTRACTORS = [ImcceExtractor,
                                    NssdcExtractor,
                                    WikidataExtractor]

    AUTHORITATIVE_EXTRACTORS = set(AVAILABLE_EXTRACTORS) - set(NON_AUTHORITATIVE_EXTRACTORS)
