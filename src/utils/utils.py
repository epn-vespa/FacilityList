"""
Utility functions for the project.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import os
from config import TMP_DIR

def clear_tmp():
    import shutil
    for file in os.listdir(TMP_DIR):
        file_path = TMP_DIR / file
        if os.path.isfile(file_path):
            os.remove(file_path)
        else:
            shutil.rmtree(file_path)
