import tomllib
from pathlib import Path

from finalyze import APP_NAME

CONFIG_DIR = Path.home() / ".config" / APP_NAME.lower()
CONFIG_FILE = CONFIG_DIR / "config.toml"
DEFAULT_CONFIG_CONTENTS = """
[cli]
global = []
"""


def load_config():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_FILE.is_file():
        CONFIG_FILE.write_text(DEFAULT_CONFIG_CONTENTS)
    return tomllib.loads(CONFIG_FILE.read_text())
