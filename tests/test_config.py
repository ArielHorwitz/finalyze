from pathlib import Path

import pydantic
import pytest

from finalyze import config as config_module


def test_write_default_config_defaults_to_config_dir():
    config_module.write_default_config()
    assert config_module.CONFIG_FILE.is_file()


def test_default_config_round_trips(tmp_path):
    config_module.write_default_config(config_dir=tmp_path)
    loaded = config_module.load_config(config_dir=tmp_path, use_preloaded=False)
    assert loaded == config_module.validate_config({})
    # Without a host file, the defaults
    host = config_module.host()
    assert host.data_dir == Path.home().joinpath(".local", "share", "finalyze")
    assert host.raw_dir == host.data_dir.joinpath("raw")
    assert (host.host, host.port) == (None, None)
    assert not tmp_path.joinpath("host.toml").exists()


def test_relative_host_paths_are_of_the_config_dir(tmp_path, monkeypatch):
    config_module.write_default_config(config_dir=tmp_path)
    tmp_path.joinpath("host.toml").write_text('data_dir = "data"\nraw_dir = "../raw"\n')
    monkeypatch.chdir("/")
    config_module.load_config(config_dir=tmp_path, use_preloaded=False)
    assert config_module.host().data_dir == tmp_path.joinpath("data")
    assert config_module.host().raw_dir == tmp_path.parent.joinpath("raw")


def test_host_file(tmp_path):
    config_module.write_default_config(config_dir=tmp_path)
    tmp_path.joinpath("host.toml").write_text(
        'data_dir = "~/finances"\nraw_dir = "~/exports/raw"\n'
        'host = "0.0.0.0"\nport = 9000\n'
    )
    config_module.load_config(config_dir=tmp_path, use_preloaded=False)
    host = config_module.host()
    assert host.data_dir == Path.home().joinpath("finances")
    assert host.raw_dir == Path.home().joinpath("exports", "raw")
    assert host.db_file == host.data_dir.joinpath("finalyze.db")
    assert host.backups_dir == host.data_dir.joinpath("backups")
    assert (host.host, host.port) == ("0.0.0.0", 9000)
    # The raw folder defaults to the data directory's
    tmp_path.joinpath("host.toml").write_text('data_dir = "/srv/finalyze"\n')
    config_module.load_config(config_dir=tmp_path, use_preloaded=False)
    assert config_module.host().raw_dir == Path("/srv/finalyze/raw")
    tmp_path.joinpath("host.toml").write_text('data_dri = "/srv/finalyze"\n')
    with pytest.raises(pydantic.ValidationError, match="data_dri\n  Extra inputs"):
        config_module.load_config(config_dir=tmp_path, use_preloaded=False)


@pytest.mark.parametrize("raw_dir", ["/srv/finalyze", "/srv", "/srv/finalyze/."])
def test_the_raw_folder_cant_hold_the_data_directory(tmp_path, raw_dir):
    """An import replaces everything in the raw folder."""
    config_module.write_default_config(config_dir=tmp_path)
    tmp_path.joinpath("host.toml").write_text(
        f'data_dir = "/srv/finalyze"\nraw_dir = "{raw_dir}"\n'
    )
    with pytest.raises(pydantic.ValidationError, match="can't be the data directory"):
        config_module.load_config(config_dir=tmp_path, use_preloaded=False)


@pytest.mark.parametrize(
    "text, setting",
    [
        ('[general]\ndata_dir = "~/finances"\n', "general.data_dir"),
        ('[ingestion.directories]\nalice = ["~/raw/alice"]\n', "ingestion.directories"),
    ],
)
def test_paths_moved_out_of_the_config_are_explained(tmp_path, text, setting):
    tmp_path.joinpath("config.toml").write_text(text)
    with pytest.raises(pydantic.ValidationError) as raised:
        config_module.load_config(config_dir=tmp_path, use_preloaded=False)
    assert f"{setting} is no longer in the config: remove it" in str(raised.value)
    assert "host.toml" in str(raised.value)


def test_both_moved_paths_are_named_at_once(tmp_path):
    tmp_path.joinpath("config.toml").write_text(
        '[general]\ndata_dir = "~/finances"\n\n'
        '[ingestion.directories]\nalice = ["~/raw/alice"]\n'
    )
    with pytest.raises(pydantic.ValidationError) as raised:
        config_module.load_config(config_dir=tmp_path, use_preloaded=False)
    assert (
        "general.data_dir and ingestion.directories are no longer in the config: "
        "remove them"
    ) in str(raised.value)


@pytest.mark.parametrize(
    "config_data",
    [
        {"analysis": {"breakdown_month": 2}},
        {"analysis": {"graphs": {"titel": "plots"}}},
        {"analysis": {"external_filters": {"acount": "savings"}}},
        {"tag": {"preset_rules": [{"tag": "food", "filter": {}}]}},
        {"unknown_section": {}},
    ],
)
def test_unknown_config_keys_are_rejected(config_data):
    with pytest.raises(
        pydantic.ValidationError, match="Extra inputs are not permitted"
    ):
        config_module.Config(**config_data)


def test_removed_settings_are_named_when_loading(tmp_path):
    """A config written for an older version fails, naming each setting that no
    longer exists (here, the removed backups and terminal display)."""
    tmp_path.joinpath("config.toml").write_text(
        '[general]\nbackup_dir = "~/backups"\nmaximum_backups = 10\n'
        "[display]\nflip_rtl = true\n"
    )
    with pytest.raises(pydantic.ValidationError) as raised:
        config_module.load_config(config_dir=tmp_path, use_preloaded=False)
    message = str(raised.value)
    for setting in ("general", "display"):
        assert f"{setting}\n  Extra inputs are not permitted" in message
