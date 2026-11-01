import shutil

import pytest

from finalyze import config as config_module
from finalyze import demo
from finalyze.config import host
from finalyze.service import open_service
from finalyze.store import Store
from tests.helpers import (
    DEMO_END_DATE,
    make_isolated_config,
    make_isolated_host,
    relocate_demo,
)


@pytest.fixture
def use_config(monkeypatch):
    """Install a config as the cached global returned by `config()`."""

    def install(new_config: config_module.Config) -> config_module.Config:
        monkeypatch.setattr(config_module, "LOADED_CONFIG", new_config)
        return new_config

    return install


@pytest.fixture
def use_host(monkeypatch):
    """Install host settings as the cached global returned by `host()`."""

    def install(new_host: config_module.Host) -> config_module.Host:
        monkeypatch.setattr(config_module, "LOADED_HOST", new_host)
        return new_host

    return install


@pytest.fixture(autouse=True)
def isolated_config(use_config, use_host, tmp_path, monkeypatch):
    """Every test starts with an isolated default config and host settings
    (paths under its temporary directory) instead of loading them, and writes
    to a config file (e.g. through the API) go to its temporary directory, never
    to the user's."""
    monkeypatch.setattr(config_module, "LOADED_CONFIG_DIR", tmp_path.joinpath("config"))
    use_host(make_isolated_host(tmp_path))
    return use_config(make_isolated_config(tmp_path))


@pytest.fixture(scope="session")
def demo_template(tmp_path_factory):
    """The demo, built (seeded, ingested and annotated) once."""
    root = tmp_path_factory.mktemp("demo-template")
    demo.build(root, end_date=DEMO_END_DATE)
    return root


@pytest.fixture
def demo_config(demo_template, tmp_path, isolated_config):
    """A fresh copy of the demo, loaded as the config (and its host file)."""
    root = tmp_path.joinpath("demo")
    shutil.copytree(demo_template, root)
    return relocate_demo(demo_template, root)


@pytest.fixture
def service(demo_config):
    with open_service() as service:
        yield service


@pytest.fixture
def unannotated_demo(demo_config):
    """The demo without its annotations: only its rules tag transactions."""
    with Store(host().db_file) as store:
        store.delete_annotations(
            annotation.transaction_id for annotation in store.list_annotations()
        )
    return demo_config
