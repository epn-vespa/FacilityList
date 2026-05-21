import setup_path

import unittest

from graph.entity import Entity
from graph import entity_types
from graph.graph import Graph
from graph.properties import Properties
from views import post_process
from rdflib import URIRef, Literal
import json

p = Properties()

class TestLabelStandardization(unittest.TestCase):

    telescope = Entity(URIRef("telescope"))
    telescope.data = {p.label: "Telescope A",
                      p.type: {entity_types.Telescope, entity_types.Spacecraft},
                      p.aperture: "0.5m"}
    
    space_instrument = Entity(URIRef("space_instrument"))
    space_instrument.data = {p.label: "Space instrument 1",
                             p.type: entity_types.Instrument,
                             p.is_part_of: telescope}

    observatory = Entity(URIRef("observatory"))
    observatory.data = {p.label: "Observatory A",
                        p.type: entity_types.GroundObservatory,
                        p.country: "France"}

    obs_telescope = Entity(URIRef("obs_telescope"))
    obs_telescope.data = {p.label: "Telescope B",
                          p.type: entity_types.Telescope,
                          p.aperture: "0.7m",
                          p.is_part_of: observatory}

    ground_instrument = Entity(URIRef("ground_instrument"))
    ground_instrument.data = {p.label: "Ground instrument 1",
                              p.type: entity_types.Instrument,
                              p.is_part_of: obs_telescope}

    agency = Entity(URIRef("agency"))
    agency.data = {p.label: "ESA telescope",
                   p.type: entity_types.Telescope}

    pp = post_process.PostProcess(graph = None)

    def graph(*args):
        graph = Graph(None)
        for arg in args:
            for k, vv in arg.data.items():
                for v in vv:
                    graph.add((arg.uri, k, v))
        return graph
    graph = graph(telescope, space_instrument)

    def print_label_warnings(self):
        print("Label warnings:", json.dumps(self.pp.label_warnings, indent = 2))
    """
    def test_orphan_telescope_no_aperture(self):
        pp = self.pp
        pp._check_llm_label(self.telescope, "0.5m A telescope")
        warnings = set(pp.label_warnings["telescope"])
        assert warnings == {"orphan_entity"}

        pp._check_llm_label(self.telescope, "A telescope")
        warnings = set(pp.label_warnings["telescope"])
        assert warnings == {"orphan_entity", "missing_aperture"}

        pp._check_llm_label(self.telescope, "6 inches A telescope")
        warnings = set(pp.label_warnings["telescope"])
        self.print_label_warnings()
        assert "aperture_format" in warnings
    def test_space_instrument(self):
        pp = self.pp

        pp._check_llm_label(self.space_instrument, "Space Instrument 1 on Telescope A")
        self.print_label_warnings()
        warnings = pp.label_warnings["space_instrument"]
        print(set(warnings))
        assert not set(warnings)
        pp._check_llm_label(self.space_instrument, "Space Instrument 1 on")
        warnings = pp.label_warnings["space_instrument"]
        self.print_label_warnings()
        assert set(warnings) == {"broader_format", "broader_missing"}
    
    def test_ground_instrument(self):
        pp = self.pp
        pp._check_llm_label(self.ground_instrument, "Ground instrument 1 on Telescope B")
        self.print_label_warnings()
        warnings = pp.label_warnings["space_instrument"]
        print(set(warnings))
        assert not set(warnings)
        pp._check_llm_label(self.space_instrument, "Space Instrument 1 on")
        warnings = pp.label_warnings["space_instrument"]
        self.print_label_warnings()
        assert set(warnings) == {"broader_format", "broader_missing"}

    def test_obs_telescope(self):
        pp = self.pp
        pp._check_llm_label(self.obs_telescope, "0.77m Telescope B at Observatory A, France")
        warnings = pp.label_warnings["obs_telescope"]
        print("Warnings:", warnings)
        assert not set(warnings)
        pp._check_llm_label(self.obs_telescope, "0.77m Telescope B at Observatory A")
        warnings = pp.label_warnings["obs_telescope"]
        assert set(warnings) == {"country_missing"}
    """

    def test_agency(self):
        pp = self.pp
        pp._check_llm_label(self.agency, "0.77m ESA Telescope")
    

if __name__ == "__main__":
    unittest.main()
