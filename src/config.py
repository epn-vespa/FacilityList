"""
Configuration script. Use to change the project's parameters.
This script will setup a shuttle to tycho and armstrong to access Ollama.

Author:
    Liza Fretel (liza.fretel@obspm.fr)
"""
from pathlib import Path
import os
import atexit
import asyncio

# directories
ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data"
LOGS_DIR = ROOT / "logs"
CONF_DIR = ROOT / "conf"
OUTPUT_DIR = ROOT / "src" / "output"
USERNAME = os.environ.get("USER") or os.environ.get("USERNAME") or os.getlogin()
if "SSH_CONNECTION" in os.environ or "SSH_CLIENT" in os.environ:
    # tycho
    CACHE_DIR = Path("/data") / USERNAME / "cache"
    TMP_DIR = Path("/scratch") / USERNAME / "tmp"
else:
    # local
    CACHE_DIR = ROOT / "cache"
    TMP_DIR = ROOT / "tmp"
# mkdir
LOGS_DIR.mkdir(parents = True, exist_ok = True)
DATA_DIR.mkdir(parents = True, exist_ok = True)
CACHE_DIR.mkdir(parents = True, exist_ok = True)
TMP_DIR.mkdir(parents = True, exist_ok = True)


async def wait_connection(host, port, timeout=4):
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < timeout:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout = 1
            )
            writer.close()
            await writer.wait_closed()
            return True
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
            await asyncio.sleep(0.5)
    return False


# Ollama Configuration
async def connect_to_ollama():
    """
    Selects ollama models to use depending on the device.
    This function can be modified depending on the models and
    devices (ssh remote / local) that you are using.

    The function should be called once before LLM calls to prevent
    connecting to LLM in a non-LLM run (like during an update).
    """
    global OLLAMA_HOST
    global OLLAMA_MODEL
    global OLLAMA_MODEL_NAME
    if "SSH_CONNECTION" in os.environ or "SSH_CLIENT" in os.environ:
        # tycho
        OLLAMA_HOST = os.environ["OLLAMA_HOST"]
        if not OLLAMA_HOST:
            print("OLLAMA_HOST is not set. Please add to your ~/.bashrc:")
            print("export OLLAMA_HOST=\"http://{armstrong_IPV4}:11434\"")
            raise EnvironmentError("OLLAMA_HOST not set")
        OLLAMA_MODEL = "deepseek-v3:latest" # 400 GB (~12s)
        OLLAMA_MODEL_NAME = "DeepSeek-v3:671b"
        CONNECTION_MODE = "armstrong ollama"
    else:
        OLLAMA_HOST = "http://localhost:11435"
        try:
            # raise Exception # Force local (for test)
            proc = await asyncio.create_subprocess_exec(
                "ssh", "-L", "localhost:11435:145.238.151.114:11434",
                f"{USERNAME}@tycho.obspm.fr", "-N",#../setup.sh", USERNAME],
                stdout = asyncio.subprocess.DEVNULL,
                stderr = asyncio.subprocess.DEVNULL
            )
            if not await wait_connection("localhost", 11435, timeout=10):
                proc.terminate()
                raise TimeoutError("SSH shuttle connection timeout")
            OLLAMA_MODEL = "deepseek-v3:latest" # 400 GB (~12s)
            OLLAMA_MODEL_NAME = "DeepSeek-v3:671b"
            CONNECTION_MODE = "armstrong ollama via tycho shuttle & redirection to local port"
            atexit.register(proc.terminate)
        except Exception as e:
            # https://ceur-ws.org/Vol-3931/paper4.pdf recommands Orca2 for 7b LLMs
            print(f"Shuttle to tycho & armstrong failed with error: {e}")
            # local
            port = 11434
            OLLAMA_HOST = f"http://localhost:{port}"
            OLLAMA_MODEL = "gemma3:4b"#"gemma3:12b"#"orca2:7b"#"ministral-3:14b"
            OLLAMA_MODEL_NAME = "gemma3:4b"#"gemma3:12b"#"orca2:7b"#"ministral-3:14b"
            CONNECTION_MODE = "local ollama"
    return OLLAMA_HOST, OLLAMA_MODEL, OLLAMA_MODEL_NAME, CONNECTION_MODE

OLLAMA_HOST, OLLAMA_MODEL, OLLAMA_MODEL_NAME = None, None, None
def configure_ollama():
    global OLLAMA_HOST
    global OLLAMA_MODEL
    global OLLAMA_MODEL_NAME
    OLLAMA_HOST, OLLAMA_MODEL, OLLAMA_MODEL_NAME, CONNECTION_MODE = asyncio.run(connect_to_ollama())
    print(f"Connected to {CONNECTION_MODE}. Using model {OLLAMA_MODEL}")

OLLAMA_TEMPERATURE = 0 # Higher temperature = less determinist
ALLOW_BROAD_NARROW_MATCH = False # This will add difficulty to the classification (same, distinct, narrow, broad)

# LLM computation result files
LLM_CATEGORIES_FILE = CACHE_DIR / "llm_categories.json"
LLM_EMBEDDINGS_FILE = CACHE_DIR / f"llm_embeddings_{OLLAMA_MODEL}.pkl"


# HuggingFace, sentence transformers environment variables
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"
os.environ["HF_HOME"] = str(CACHE_DIR / "huggingface" ) # Must import before transformers
SENTENCE_TRANSFORMERS_MODEL = "UniverseTBD/astrollama"

PROMPT_SAME_DISTINCT = """Say weither those entities are the same or distinct. Justify.
Examples:
response: same. justification: DEEP SPACE 1 is a different name for VIKING 2 ORBITER.
response: same. justification: same place, same year of construction.
response: distinct. justification: entity 1 is a telescope that is part of entity 2.
response: distinct. justification: Mauna Kea and Mauna Loa are different observatories.
response: distinct. justification: entity 1 is in Chile while entity 2 is in the USA.
response: distinct. justification: Voyager II is part of the Voyager program.
response: distinct. justification: entity 1 is a program funded by NASA, entity 2 is a program funded by JAXA.
"""