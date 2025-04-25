"""
Acronymous test
"""
import setup_path
from utils import acronymous

def __test__(label, acronym):
    print(f"-- test for {label} // {acronym} --")
    score = acronymous.proba_acronym_of(acronym, label)
    print("score:", score)


def main():
    test = [("COVID-19 Vaccines Global Access", "COVAX"), # 1
            ("National Aeronautics and Space Administration", "NASA"), # 1
            ("National Aeronautics of Space Administration", "SAONAE"), # 0
            ("Taxe sur la Valeur Ajoutée", "TVA"), # 1
            ("United Nations Educational, Scientific and Cultural Organization", "UNESCO"), # 1
            ("Société Nationale des Chemins de fer Français ", "SNCF"), # not 1 because of "fer"
            ("Société Nationale des Chemins de fer Français ", "SNCFF"), # 1
            ("System for Audio-Visual Event Modeling", "SAVEM"), # 1
            ("System for Audio-Visual Event Modeling", "SyfAuViEvMo"), # 1
            ("SUMmarization in Open Context", "SUMINO" ), # ~0.6 because of Context
            ("Développement et Administration Internet et Intranet", "DA2I"), # 1 after 2*i
            ("extensible Markup Language", "XML"), # 1
            ("Southern Photometric Local Universe Survey", "S-PLUS"), # 1
            ("Deep Space Station 43", "DSS-43"), # 1
            ("Deep Space Station 43", "DSS-42"), # 0
            #("Telescopea Action Rapide pour les Objets Transitoires, Rapid Action Telescope for transient objects at Calern Observatory", "Explorer 32"),
            #("international vlbi service for geodesy and astrometry", "SONMIANI"),
            #("international vlbi service for geodesy and astrometry", "IAGA:SON"),
    ]
    for label, acronym in test:
        __test__(label, acronym)

if __name__ == "__main__":
    # Test
    main()
