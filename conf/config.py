# ***************** SCRIPT CONFIGURATION - change with care! ******************
from pathlib import Path
import os

# directories
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
LOGS_DIR = ROOT / "logs"
CONF_DIR = ROOT / "conf"
if "SSH_CONNECTION" in os.environ or "SSH_CLIENT" in os.environ:
    # tycho
    USERNAME = os.environ.get("USER") or os.environ.get("USERNAME") or os.getlogin()
    CACHE_DIR = Path("/data") / USERNAME / "cache"
else:
    # local
    CACHE_DIR = ROOT / "cache"


# Ollama Configuration
if "SSH_CONNECTION" in os.environ or "SSH_CLIENT" in os.environ:
    # tycho
    try:
        OLLAMA_HOST = os.environ["OLLAMA_HOST"]
    except:
        print("OLLAMA_HOST is not an environment variable.")
        print("To add OLLAMA_HOST in your environment, add in your ~/.bashrc:")
        print("export OLLAMA_HOST=\"http://{armstrong_IPV4}:11434\"")
    # OLLAMA_MODEL = 'llama3.3:latest' # 'fr', 'it', 'pt', 'hi', 'es', 'th', 'en'
    OLLAMA_MODEL = 'deepseek-v3:latest' # 400 GB (~12s)
    # OLLAMA_MODEL = 'gemma3:27b'
else:
    # local
    OLLAMA_HOST = "http://localhost:11434"
    OLLAMA_MODEL = 'gemma3:4b'
OLLAMA_TEMPERATURE = 0.7 # Higher temperature = less determinist


# LLM computation result files
LLM_CATEGORIES_FILE = CACHE_DIR / "llm_categories.json"
LLM_EMBEDDINGS_FILE = CACHE_DIR / f"llm_embeddings_{OLLAMA_MODEL}.json"


# precision of longitude/latitude comparison
precision = 3 # km distance ? digits after comma ?