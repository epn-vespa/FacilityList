"""
Utility functions for the project.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
import os
import signal
from config import TMP_DIR


def clear_tmp():
    import shutil
    for file in os.listdir(TMP_DIR):
        file_path = TMP_DIR / file
        if os.path.isfile(file_path):
            os.remove(file_path)
        else:
            shutil.rmtree(file_path)

class IgnoreCtrlC:
    def __enter__(self):
        self.old_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

    def __exit__(self, exc_type, exc, tb):
        signal.signal(signal.SIGINT, self.old_handler)