import streamlit as st
import os, sys

st.set_page_config(page_title="5.1 Stat Viz", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

Statistical Visual ideas
tracks per film distribution (nunique track_id)
Top 10 composers

