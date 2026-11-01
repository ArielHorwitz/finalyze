import datetime
from pathlib import Path

from finalyze import config as config_module

DEMO_END_DATE = datetime.date(2025, 6, 15)
"""The demo data of the tests ends on this day."""


def make_isolated_config(root: Path, **sections) -> config_module.Config:
    """A config with the given sections (defaults otherwise)."""
    return config_module.Config(**sections)


def make_isolated_host(root: Path, **settings) -> config_module.Host:
    """Host settings whose data directory and raw folder are under `root` (no
    accounts until folders are made in `root/raw`)."""
    return config_module.Host(
        **{"data_dir": root.joinpath("data"), "raw_dir": root.joinpath("raw")}
        | settings
    )


def relocate_demo(built_root: Path, root: Path) -> config_module.Config:
    """Load the demo built in `built_root` and copied to `root`, with its host
    file's paths moved there."""
    host_file = config_module.host_file_of(root)
    host_file.write_text(host_file.read_text().replace(str(built_root), str(root)))
    return config_module.load_config(config_dir=root, use_preloaded=False)
