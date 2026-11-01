"""The one entry point: `finalyze` serves the web app and the HTTP API."""

import argparse
import sys
import tempfile
from pathlib import Path
from typing import Optional

import pydantic
import toml

from finalyze import APP_DESCRIPTION, APP_NAME, demo
from finalyze.config import (
    CONFIG_DIR,
    HOST_FILE_NAME,
    host,
    load_config,
    write_default_config,
)
from finalyze.demo import temporary

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEMO_BANNER = """
==========================================================================
  DEMO: serving an invented household, not your data.
  It was generated into {config_dir}, which is deleted when the
  server stops: every change made in the app is lost with it.
  Your own config and data are neither read nor written.
==========================================================================
"""


def parse_args():
    parser = argparse.ArgumentParser(
        prog=APP_NAME,
        description=f"{APP_DESCRIPTION}: serve the web app and its HTTP API",
    )
    parser.add_argument(
        "-C",
        "--config-dir",
        help=f"Directory of the config file, and of the host file ({HOST_FILE_NAME}: "
        f"where the data is) [default: {CONFIG_DIR}]",
        type=Path,
    )
    parser.add_argument(
        "--host",
        help=f"The address to serve on [default: the host file's, or {DEFAULT_HOST}]",
    )
    parser.add_argument(
        "--port",
        type=int,
        help=f"The port to serve on [default: the host file's, or {DEFAULT_PORT}]",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Serve an invented household instead of your data, generated into a "
        "temporary directory that is deleted when the server stops. Your config "
        "and data are neither read nor written",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.demo:
        if args.config_dir is not None:
            sys.exit("--demo has its own config: it takes no -C")
        with temporary.directory() as temp_dir:
            # (the temporary directory has the lock file, and the demo wants an
            # empty one)
            demo_dir = temp_dir.joinpath("demo")
            print("Generating the demo household...")
            demo.build(demo_dir)
            load_config(config_dir=demo_dir, use_preloaded=False)
            serve(
                host=args.host or DEFAULT_HOST,
                port=args.port or DEFAULT_PORT,
                demo_dir=demo_dir,
            )
        return
    config_dir = args.config_dir or CONFIG_DIR
    write_default_config(config_dir=config_dir)
    try:
        load_config(config_dir=config_dir)
    except (toml.TomlDecodeError, pydantic.ValidationError) as error:
        sys.exit(f"Config error: {error}")
    host().create_directories()
    _use_data_dir_for_temporary_files()
    serve(
        host=args.host or host().host or DEFAULT_HOST,
        port=args.port or host().port or DEFAULT_PORT,
    )


def _use_data_dir_for_temporary_files():
    """Uploads being received are spooled to temporary files: in the data
    directory, rather than the system's (often in memory), with the room the
    data has. Leftovers of a server that stopped abruptly are removed first."""
    from finalyze.service import archive

    archive.remove_leftovers()
    temporary_dir = host().data_dir.joinpath(".tmp")
    temporary_dir.mkdir()
    tempfile.tempdir = str(temporary_dir)


def serve(*, host: str, port: int, demo_dir: Optional[Path] = None):
    """Serve until stopped. `demo_dir`: the demo being served, if it is one."""
    import uvicorn

    from finalyze.api import (
        DEFAULT_STATIC_DIR,
        LOCAL_HOSTS,
        MISSING_WEB_APP,
        create_app,
        stale_web_app,
    )

    if DEFAULT_STATIC_DIR.joinpath("index.html").is_file():
        print(f"Web app: http://{host}:{port}/")
        if stale := stale_web_app(DEFAULT_STATIC_DIR):
            print(f"\nWARNING: {stale}\n", file=sys.stderr)
    else:
        print(f"{MISSING_WEB_APP} Looked in: {DEFAULT_STATIC_DIR}", file=sys.stderr)
    print(f"API docs: http://{host}:{port}/docs")
    if demo_dir is not None:
        print(DEMO_BANNER.format(config_dir=demo_dir), flush=True)
    # Also accept the host served on, e.g. a LAN address chosen explicitly
    allowed_hosts = (*LOCAL_HOSTS, host)
    app = create_app(allowed_hosts=allowed_hosts, demo=demo_dir is not None)
    uvicorn.run(app, host=host, port=port)
