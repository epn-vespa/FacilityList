"""
Map ontologies with a HNSW (Hierarchical Navigable Small Worlds) approach.
Does not need a mapping strategy: the strategy is determined automatically.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from __version__ import __version__

import os
import sys
import uuid
import time
import atexit
import dill

from argparse import ArgumentParser
from pathlib import Path
from collections import defaultdict

from config import OUTPUT_DIR, configure_ollama
from graph import entity_types
from graph.graph import Graph
from graph.mapping_graph import MappingGraph
from graph.extractor.extractor_lists import ExtractorLists
from graph.extractor.naif_extractor import NaifExtractor
from graph.extractor.wikidata_extractor import WikidataExtractor
from graph.extractor.iaumpc_extractor import IauMpcExtractor
from graph.extractor.imcce_extractor import ImcceExtractor
from graph.extractor.nssdc_extractor import NssdcExtractor
from graph.extractor.pds_extractor import PdsExtractor
from data_mapper.attribute_matcher import AttributeMatcher
from data_mapper.tools.mapping_tools_list import MappingToolsList
from data_mapper.tools.filters.distance_filter import DistanceFilter
from data_mapper.hybrid_retriever import HybridRetriever


class OntologyMapper():


    def __init__(self,
                 input_ontologies: list[str],
                 output_dir: str = "",
                 human_validation: bool = False,
                 limit: int = -1):
        """
        Args:
            input_ontologies: list of ontologies to be merged
            output_dir: folder to save the output turtle files
            limit: maximum entities per list (for debug)
        """
        self._mapping_input_file = None
        restored = False
        for input_ontology in input_ontologies:
            # Try to restore the progress from a folder
            if os.path.isdir(input_ontology):
                if len(input_ontologies) > 1:
                    raise ArgumentParser.error("Can not restore mapping from multiple checkpoint folders. Please use one checkpoint folder or use ttl files as input.")
                input_ontology = Path(input_ontology)
                linked = input_ontology / "linked.ttl"
                mapping = input_ontology / "mapping.ttl"
                progress = input_ontology / "progress.pkl"
                self._graph = Graph([linked])
                if os.path.exists(mapping):
                    self._mapping_input_file = mapping
                if os.path.exists(progress):
                    with open(progress, "rb") as file:
                        self._progress = dill.load(file)
                    restored = True
                else:
                    print(f"Warning: the checkpoint folder might be malformated (no progress.pkl file in {input_ontology}). Starting strategy from scratch...")
        if not restored:
            # Instanciate the Graph from unlinked turtle file(s).
            self._graph = Graph(input_ontologies)
            self._progress = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        if not output_dir:
            output_dir = time.strftime("%Y%m%d-%H%M%S")
        self._output_dir = OUTPUT_DIR / output_dir
        self._execution_id = str(uuid.uuid4())
        self._limit = limit
        self._description = f"script: {os.path.basename(sys.argv[0])}\n" + \
            f"execution id: {self._execution_id}\n" + \
            f"source: {' '.join(input_ontologies)}\n" + \
            f"folder: {output_dir}\n"
        self._strategy = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self._human_validation = human_validation


    @property
    def strategy(self):
        return self._strategy


    def merge_identifiers(self):
        """
        Merge identifiers from different namespaces. This is done
        regardless of the strategy, depending on which namespaces
        are in the provided input ontologies.
        This step is ignored if the run was loaded from a checkpoint
        and the checkpoint's progress is not empty.
        """
        if self._progress:
            return
        am = AttributeMatcher()

        if (self._graph.is_available("naif") and
            self._graph.is_available("wikidata")):
            # CPM_wiki_naif = CandidatePairsManager(WikidataExtractor(),
            #                                       NaifExtractor())
            #CPM_wiki_naif = CandidatePairsMapping(WikidataExtractor,
            #                                      NaifExtractor)

            # merge wiki naif if the namespaces are available.
            am.merge_wikidata_naif()

            self._description += "merge identifiers: naif, wikidata\n"
            # del(CPM_wiki_naif)
        if (self._graph.is_available("iaumpc") and
            self._graph.is_available("wikidata")):
            # P717. Doublon: P5736

            am.merge_on(list1 = WikidataExtractor(),
                        list2 = IauMpcExtractor(),
                        attr1 = "MPC_ID",
                        attr2 = "code")
            # CPM_wiki_iaumpc.compute_tools()
            self._description += "merge identifiers: iaumpc, wikidata\n"
        if (self._graph.is_available("nssdc") and
            self._graph.is_available("wikidata")):
            # P247 (COSPAR ID) Doublon: P8913 (NSSDCA)
            # Some of them appear more than once in wikidata.

            am.merge_on(list1 = WikidataExtractor(),
                        list2 = NssdcExtractor(),
                        attr1 = "NSSDCA_ID",
                        attr2 = "code")
            am.merge_on(list1 = WikidataExtractor(),
                        list2 = NssdcExtractor(),
                        attr1 = "COSPAR_ID",
                        attr2 = "code")
            self._description += "merge identifiers: nssdc, wikidata\n"
        if (self._graph.is_available("imcce") and
            self._graph.is_available("wikidata")):
            am.merge_on(list1 = WikidataExtractor(),
                        list2 = ImcceExtractor(),
                        attr1 = "NSSDCA_ID",
                        attr2 = "alt_label")
            am.merge_on(list1 = WikidataExtractor(),
                        list2 = ImcceExtractor(),
                        attr1 = "COSPAR_ID",
                        attr2 = "alt_label")
            self._description += "merge identifiers: imcce, wikidata\n"
        if (self._graph.is_available("imcce") and
            self._graph.is_available("nssdc")):
            am.merge_on(list1 = NssdcExtractor(),
                        list2 = ImcceExtractor(),
                        attr1 = "code",
                        attr2 = "alt_label")
            self._description += "merge identifiers: nssdc, imcce\n"
        if (self._graph.is_available("pds") and
            self._graph.is_available("nssdc")):
            am.merge_on(list1 = NssdcExtractor(),
                        list2 = PdsExtractor(),
                        attr1 = "alt_label",
                        attr2 = "code")
            self._description += "merge identifiers: pds, nssdc\n"
        if (self._graph.is_available("naif") and
            self._graph.is_available("pds")):
            am.merge_on(list1 = NaifExtractor(),
                        list2 = PdsExtractor(),
                        attr1 = "code",
                        attr2 = "NAIF_ID")
            self._description += "merge identifiers: pds, naif\n"
        del(am)


    def parse_strategy(self,
                       strategy_file: str):
        """
        Parse a mapping strategy file. The strategy is a list of
        mapping operations to be executed in order.
        '#' can be used for comments.
        Each line must have the following format:
        list1, list2 [type1, type2, -type3]: tool1, tool2, -tool3
        with exactly two lists, separated by a comma,
        followed by an optional list of types between square brackets,
        followed by a colon and a list of tools to use, separated by commas.
        A type can be excluded by prefixing it with a '-'.
        A tool can be excluded by prefixing it with a '-'.
        The special type 'all' can be used to select all available types.
        The special tool 'all' can be used to select all available tools.

        The progress dict is removed from the parsed strategy
        if a checkpoint is restored.

        Args:
            strategy_file: path to the strategy file
        """
        # Re-initialize the strategy
        self._strategy = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        with open(strategy_file, 'r') as file:
            for i, line in enumerate(file.readlines()):
                i = i + 1
                line = line.split('#')[0].strip()
                if not line:
                    continue # skip line if empty or comment line
                if line.count(':') != 1:
                    raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                     f"There must be exactly one ':' per line.")
                extractors, tools = line.split(':')
                if not tools.strip():
                    raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                     f"No tool set.")
                 # Types
                on_types = set(entity_types.ALL_TYPES)
                begin_types = extractors.find("[")
                if begin_types > 0:
                    end_types = extractors.find("]")
                    if end_types < begin_types:
                        raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                         f"[ and ] do not match. There must be exactly one [ and one ].")
                    types_str = extractors[begin_types+1:end_types]
                    on_types_lst = set(types_str.split(','))
                    extractors = extractors[:begin_types] + extractors[end_types+1:].strip()
                    except_types = set()
                    on_types = set()
                    for type_ in on_types_lst:
                        type_ = type_.strip()
                        if type_ == "all":
                            on_types = set(entity_types.ALL_TYPES)
                            continue
                        elif type_.startswith('-'):
                            type_ = type_[1:].strip()
                            except_types.add(type_)
                        else:
                            on_types.add(type_)
                        if type_ not in entity_types.ALL_TYPES:
                            raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                             f"{type_} is not a valid type.\n" +
                                             f"Valid types are: {' '.join(entity_types.ALL_TYPES)}")
                    on_types -= except_types
                if len(on_types) == 0:
                    print(f"Warning at line {i} in {strategy_file}: " +
                          f"No types selected. Ignoring.")
                    continue
                extractors = extractors.split(',')
                extractors = [e.strip() for e in extractors if e.strip()]
                if len(extractors) != 2:
                    raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                     f"There must be two list names per line.")
                extractor1_str = extractors[0]
                extractor2_str = extractors[1]

                if extractor2_str < extractor1_str:
                    extractor2_str, extractor1_str = extractor1_str, extractor2_str

                if extractor1_str not in ExtractorLists.EXTRACTORS_BY_NAMES.keys():
                    raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                     f"{extractor1_str} is not a valid list name.\n" +
                                     f"Available list names: {' '.join(ExtractorLists.EXTRACTORS_BY_NAMES.keys())}")
                extractor1 = ExtractorLists.EXTRACTORS_BY_NAMES[extractor1_str]
                if extractor2_str not in ExtractorLists.EXTRACTORS_BY_NAMES.keys():
                    raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                     f"{extractor2_str} is not a valid list name.\n" +
                                     f"Available list names: {' '.join(ExtractorLists.EXTRACTORS_BY_NAMES.keys())}")
                extractor2 = ExtractorLists.EXTRACTORS_BY_NAMES[extractor2_str]

                if not (extractor1.POSSIBLE_TYPES.intersection(extractor2.POSSIBLE_TYPES)):
                    print(f"Warning at line {i} in {strategy_file}: " +
                          f"No type intersection between {extractor1.NAMESPACE} and {extractor2.NAMESPACE}. Ignoring.")
                if begin_types < 0:
                    on_types = extractor1.POSSIBLE_TYPES.intersection(extractor2.POSSIBLE_TYPES)

                tools = tools.split(',')
                tools = [s.strip() for s in tools if s.strip()]
                tools_to_compute = set()
                except_tools = set()
                for tool in tools:
                    if tool[0] == '-':
                        # Do not compute those tools
                        tool = tool[1:].strip()
                        if tool not in MappingToolsList.TOOLS_BY_NAMES:
                            raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                             f"{tool} is not a valid tool name.\n" +
                                             f"Available tool names: {' '.join(MappingToolsList.TOOLS_BY_NAMES.keys())}")
                        else:
                            except_tools.add(MappingToolsList.TOOLS_BY_NAMES[tool])
                    elif tool == 'all':
                        tools_to_compute.update(MappingToolsList.ALL_TOOLS)
                    elif tool not in MappingToolsList.TOOLS_BY_NAMES.keys():
                        raise ValueError(f"Error at line {i} in {strategy_file}: " +
                                         f"{tool} is not a valid tool name.\n" +
                                         f"Available tool names: {' '.join(MappingToolsList.TOOLS_BY_NAMES.keys())}")
                    else:
                        tools_to_compute.add(MappingToolsList.TOOLS_BY_NAMES[tool])
                    if DistanceFilter in tools_to_compute:
                        if ((entity_types.GROUND_OBSERVATORY not in extractor1.POSSIBLE_TYPES and
                             entity_types.TELESCOPE not in extractor1.POSSIBLE_TYPES) or
                            (entity_types.GROUND_OBSERVATORY not in extractor2.POSSIBLE_TYPES and
                             entity_types.TELESCOPE not in extractor2.POSSIBLE_TYPES)):
                            tools_to_compute.remove(DistanceFilter)
                            print(f"Warning at line {i} in {strategy_file}: " +
                                  f"Can not compute distance between {extractor1_str} and {extractor2_str}. Ignoring.")
                if (self._graph.is_available(extractor1_str) and
                    self._graph.is_available(extractor2_str)):

                    tools = tools_to_compute - except_tools
                    if tools:
                        if extractor2_str < extractor1_str:
                            extractor1, extractor2 = extractor2, extractor1
                            extractor1_str, extractor2_str = extractor2_str, extractor1_str
                        #for type in on_types:
                        self.strategy[extractor1][extractor2][frozenset(on_types)] = tools
                    else:
                        print(f"Warning at line {i} in {strategy_file}: " +
                              f"No tool to compute for {extractor1_str} and {extractor2_str}. Ignoring.")
                else:
                    print(f"Warning at line {i} in {strategy_file}: " +
                          f"{extractor1_str} and/or {extractor2_str} are not available in the provided ontologies. Ignoring.")
        self._restore_progress()


    def execute_strategy(self):
        """
        Execute the mapping strategy.
        """
        # execute the strategy
        atexit.register(self.write)
        for extractor1 in self.strategy.keys():
            for extractor2 in self.strategy[extractor1].keys():

                for on_types in self.strategy[extractor1][extractor2].keys():
                    tools = self.strategy[extractor1][extractor2][on_types]
                    if not extractor1.TYPE_KNOWN == 1 or not extractor2.TYPE_KNOWN == 1:
                        on_types = [on_types] # On all types at once if types are unknown
                    # If types from both lists are known, process types one by one.
                    for on_type in on_types:
                        retriever = HybridRetriever()
                        retriever.process_lists(extractor1(),
                                                extractor2(),
                                                on_types = on_type,
                                                with_tools = list(tools),
                                                limit = self._limit,
                                                ignore_deprecated = True,
                                                human_validation = self._human_validation)
                        # Candidates is a list of (entity, score)
                        """
                        for entity1, candidates in retriever.disambiguate(extractor1(),
                                                                          extractor2(),
                                                                          on_types = on_type,
                                                                          with_tools = list(tools),
                                                                          limit = self._limit,
                                                                          ignore_deprecated = True):

                            response = LLMConnection.choose_best_candidate_and_justify(entity1, candidates)
                            best_candidate = None
                            print(response)
                            if best_candidate:
                                self._graph.add_mapping(entity1,
                                                        entity2,
                                                        score,
                                                        tools,
                                                        self._execution_id)
                                # Del entity2 from retriever index to avoid multiple mapping
                                # retriever.index.remove_ids(np.array([entity2.id], dtype = 'int64'))
                        """
                        del(retriever)

                        # Save progress for next execution
                        self._progress[extractor1][extractor2][on_type] = tools

                    self._description += f"mapping: {extractor1.NAMESPACE}, {extractor2.NAMESPACE}, types: {', '.join([str(t) for t in on_types])}, tools: {', '.join([s.NAME for s in tools])}\n"


    def _restore_progress(self):
        """
        Function that removes progress from the strategy
        to prevent re-executing strategy lines.
        """
        progress = self._progress
        if not progress:
            return
        for list1, lists2 in progress.copy().items():
            if list1 in self._strategy:
                for list2 in lists2.copy():
                    if list2 in self._strategy[list1]:
                        on_types = progress[list1][list2]
                        for on_type in on_types.copy():
                            if on_type in self._strategy[list1][list2]:
                                self._strategy[list1][list2].pop(on_type)
                                if len(self._strategy[list1][list2]) == 0:
                                    self._strategy[list1].pop(list2)
                            else:
                                print(f"Warning: strategy changed since last run (different types for {list1},{list2}).")
                        if len(self._strategy[list1]) == 0:
                            self._strategy.pop(list1)
                    else:
                        print(f"Warning: strategy changed since last run (removed {list1},{list2}).")
            else:
                print(f"Warning: strategy changed since last run (removed {list1}).")


    def write(self):
        print(f"Writing the result ontology into {self._output_dir}...")
        self._graph.add_metadata(self._description)
        output_dir = Path(self._output_dir)
        output_dir.mkdir(parents = True, exist_ok = True)
        output_ontology = output_dir / 'linked.ttl'
        self._graph.serialize(destination = output_ontology,
                              format = "turtle",
                              encoding = "utf-8")
        mapping_graph = MappingGraph(self._mapping_input_file)
        mapping_graph.serialize(output_dir = self._output_dir,
                                execution_id = self._execution_id)
        progress_file = output_dir / 'progress.pkl'
        with open(progress_file, "wb") as file:
            dill.dump(self._progress, file)
        atexit.unregister(self.write)


def main(input_ontologies: list[str],
         output_dir: str,
         strategy_file: str,
         human_validation: bool):


    mapper = OntologyMapper(input_ontologies,
                            output_dir = output_dir,
                            human_validation = human_validation)
    mapper.parse_strategy(strategy_file)
    mapper.merge_identifiers()
    if not human_validation:
        configure_ollama()
        mapper.execute_strategy()
    else:
        # Open the server & web browser client for manual disambiguation
        import threading
        from data_mapper.gui import server
        thread = threading.Thread(target = mapper.execute_strategy, daemon = True)
        thread.start()
        print("Serving on http://127.0.0.1:5000")
        server.app.run(debug = True, use_reloader = False)
    mapper.write()


if __name__ == "__main__":
    parser = ArgumentParser(prog = "map_ontologies",
                            description = "Map entities from different sources based on their embeddings.")
    parser.add_argument("-i",
                        "--input-files",
                        dest="input_ontologies",
                        nargs="+",
                        required=True,
                        type=str,
                        help="List of ontologies (ttl) to be merged or path to the checkpoint folder from last execution (containing linked.ttl, mapping.ttl and progress.pkl).")
    parser.add_argument("-o",
                        "--output-dir",
                        dest="output_dir",
                        required=False,
                        type=str,
                        default="",
                        help="Folder to save the output turtle files.")
    parser.add_argument("-s",
                        "--strategy-file",
                        dest="strategy_file",
                        required=False,
                        type=str,
                        default = str(Path(__file__).parent.parent / "conf" / "default_strategy.conf"),
                        help="Folder to save the output turtle files.")
    parser.add_argument("--human-validation",
                        dest="human_validation",
                        required=False,
                        action="store_true",
                        help="If set, will perform validation manually.")
    parser.add_argument("-v",
                        "--version",
                        action="version",
                        version=f"%(prog)s {__version__}",
                        help="Print the current version.")
    args = parser.parse_args()
    main(args.input_ontologies,
         args.output_dir,
         args.strategy_file,
         args.human_validation)
