import streamlit as st
import os, sys

st.set_page_config(page_title="PyTrends Exploration", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(DATA_DIR)
# Ensure repo root is on path so utils/ can be imported from any subdirectory
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
if DATA_DIR not in sys.path:
    sys.path.insert(0, DATA_DIR)

import urllib3, filelock
from pytrends.request import TrendReq
import pytrends

print("pytrends module path:", pytrends.__file__)
print("urllib3:", urllib3.__version__)
print("filelock:", filelock.__version__)

# If you want the package version, this is the most reliable in notebooks:
import subprocess, sys
print(subprocess.check_output([sys.executable, "-m", "pip", "show", "pytrends"]).decode().splitlines()[1])

from pytrends.request import TrendReq

pt = TrendReq(hl="en-US", tz=0, timeout=(10, 25), retries=0)

try:
    pt.build_payload(["Taylor Swift"], timeframe="today 7-d", geo="US")
    df = pt.interest_over_time()
    print(df.head())
except Exception as e:
    resp = getattr(e, "response", None)
    print("Error type:", type(e).__name__)
    if resp is None:
        print("No response attached. Message:", str(e))
    else:
        print("HTTP status:", resp.status_code)
        print("Content-Type:", resp.headers.get("Content-Type"))
        print("First 1200 chars of response:\n", resp.text[:1200])
