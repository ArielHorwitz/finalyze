import atexit
import os
import shutil
import tempfile

# Point HOME at an empty directory before anything imports finalyze, whose
# module-level defaults (config dir, data dir, source dirs) derive from the home
# directory. This guarantees tests never read or write the real config or data.
os.environ["HOME"] = tempfile.mkdtemp(prefix="finalyze-test-home-")
atexit.register(shutil.rmtree, os.environ["HOME"], ignore_errors=True)
