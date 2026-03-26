import streamlit as st
import os, sys

st.set_page_config(page_title="Init", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
## Initialization Notebook
This is your initialization notebook.

**What's this for?**

You can put custom code you want us to run every time we setup your environment in here. 

**Awesome! Anything I should not put in here?**

Please don't install `jupyter` or `jedi` packages, they would break your Deepnote environment. Also, no need to put `!pip install`s here, we already save those automatically!

**I want to learn more!**

Great! Just [head over to our docs](https://deepnote.com/docs/project-initialization).
""")
