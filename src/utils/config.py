# ***************** SCRIPT CONFIGURATION - change with care! ******************
from pathlib import Path

# directories
root = Path(__file__).parent.parent.parent
data_dir = root / "data"
cache_dir = root / "cache"
logs_dir = root / "logs"
conf_dir = root / "conf"

# precision of longitude/latitude comparison
precision = 3 # km distance ? digits after comma ?