# ***************** SCRIPT CONFIGURATION - change with care! ******************
from pathlib import Path
import os

# directories
root = Path(__file__).parent.parent.parent
data_dir = root / "data"
logs_dir = root / "logs"
conf_dir = root / "conf"
if "SSH_CONNECTION" in os.environ or "SSH_CLIENT" in os.environ:
    # tycho
    username = os.environ.get("USER") or os.environ.get("USERNAME") or os.getlogin()
    cache_dir = Path("/data") / username / "cache"
else:
    # local
    cache_dir = root / "cache"

# Ollama Configuration
if "SSH_CONNECTION" in os.environ or "SSH_CLIENT" in os.environ:
    # tycho
    try:
        OLLAMA_HOST = os.environ["OLLAMA_HOST"]
    except:
        print("OLLAMA_HOST is not an environment variable.")
        print("To add OLLAMA_HOST in your environment, add in your ~/.bashrc:")
        print("export OLLAMA_HOST=\"http://{armstrong_IPV4}:11434\"")
    OLLAMA_MODEL = 'llama3.3:latest'
else:
    # local
    OLLAMA_HOST = "http://localhost:11434"
    OLLAMA_MODEL = 'gemma3:4b'
    # MODEL = 'gemma3:1b'
    # MODEL = 'gemma3:12b'
OLLAMA_TEMPERATURE = 0.7 # Higher temperature = less determinist


# precision of longitude/latitude comparison
precision = 3 # km distance ? digits after comma ?