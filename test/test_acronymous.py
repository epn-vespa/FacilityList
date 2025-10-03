"""
Acronymous test
"""
import setup_path
from utils import acronymous
import unittest


class TestEntity(unittest.TestCase):

    def test_proba(self):
        def __test__(label, acronym, proba):
            score = acronymous.proba_acronym_of(acronym, label)
            #print(f"-- test for {label} // {acronym} --")
            #print("score:", score)
            if proba is not None:
                assert score == proba
            else:
                assert score > 0 and score < 1
        test = [("COVID-19 Vaccines Global Access", "COVAX", None),
                ("National Aeronautics and Space Administration", "NASA", 1),
                ("National Aeronautics of Space Administration", "SAONAE", None),
                ("Taxe sur la Valeur Ajoutée", "TVA", 1),
                ("United Nations Educational, Scientific and Cultural Organization", "UNESCO", 1),
                ("Société Nationale des Chemins de fer Français ", "SNCF", None), # not 1 because of "fer"
                ("Société Nationale des Chemins de fer Français ", "SNCFF", 1), # 1
                ("System for Audio-Visual Event Modeling", "SAVEM", 1), # 1
                ("System for Audio-Visual Event Modeling", "SyfAuViEvMo", 1), # 1
                ("SUMmarization in Open Context", "SUMINO", None), # ~0.6 because of Context
                ("Développement et Administration Internet et Intranet", "DA2I", 1), # 1 after 2*i
                ("extensible Markup Language", "XML", 1), # 1
                ("Southern Photometric Local Universe Survey", "S-PLUS", 1), # 1
                ("Deep Space Station 43", "DSS-43", 1), # 1
                ("Deep Space Station 43", "DSS-42", 0), # 0
                #("Telescopea Action Rapide pour les Objets Transitoires, Rapid Action Telescope for transient objects at Calern Observatory", "Explorer 32"),
                #("international vlbi service for geodesy and astrometry", "SONMIANI"),
                #("international vlbi service for geodesy and astrometry", "IAGA:SON"),
        ]
        for label, acronym, proba in test:
            __test__(label, acronym, proba)

if __name__ == "__main__":
    unittest.main()
