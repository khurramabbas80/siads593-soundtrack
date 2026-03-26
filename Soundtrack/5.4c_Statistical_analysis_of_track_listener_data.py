import streamlit as st
import os, sys

st.set_page_config(page_title="5.4c Statistical analysis of track listener data", layout="wide")

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

st.markdown("""
# I\\. Setup and Inspection
""")

#Imports
import os
from typing import List, Union
import sys

import altair as alt
import numpy as np
import pandas as pd

import holoviews as hv
import panel as pn

from bokeh.io import output_file, save
from bokeh.io import output_notebook
from bokeh.resources import INLINE

output_notebook(resources=INLINE)
hv.extension("bokeh")
pn.extension()

pd.set_option('display.max_columns', None)
pd.set_option("display.width", 200)

# Any visualization using our team_theme
import altair as alt

from utils.viz_theme import enable, sized
enable()    # You need to enable the theme in order for it to work

alt.data_transformers.disable_max_rows()

# Load the dataframes
tracks_clean_df = pd.read_csv("./pipeline/5.4.b.Tracks_tempo_composer_clean.csv")

print(f"Total records in tracks_df: {tracks_clean_df.shape[0]}")
print(tracks_clean_df.columns)
tracks_clean_df.head()

st.markdown("""
# II\\. Feature Selection for Correlation
""")

st.markdown("""
To prepare for track level correlation analysis, we will narrow our focus to the audio features that were obtained from the Soundnet API \\(e\\.g\\., tempo, happiness\\)\\.
""")

corr_cols = tracks_clean_df[[
    'log_lfm_track_playcount',
    'key',
    'mode',
    'tempo',
    'energy',
    'danceability',
    'happiness',
    'acousticness',
    'instrumentalness',
    'liveness',
    'speechiness',
    'loudness'
 ]]

# Shorten log_lfm_track_playcount column title for the heatmap

corr_cols = corr_cols.rename(columns={
    "log_lfm_track_playcount": "playcount"
})

st.markdown("""
# III\\. Create the Correlation Matrix
""")

st.markdown("""
To handle features such as musical key, we will use one\\-hot encoding before generating the matrix\\.
""")

# One-hot encode categorical columns

df_encoded = pd.get_dummies(
    corr_cols,
    columns=['key', 'mode'],
    drop_first=True
)

st.markdown("""
First, we will check for correlation using the Pearson method\\.
""")

# Compute correlation (Pearson)

corr_pearson = df_encoded.corr()
# auto-detected possible Altair chart: corr_pearson
try:
    st.altair_chart(corr_pearson, use_container_width=True)
except Exception:
    st.write(corr_pearson)

# Helper function to convert correlation matrix to long form in preparation for Altair

def corr_to_long(corr_df: pd.DataFrame) -> pd.DataFrame:
    return (
        corr_df
        .reset_index()
        .melt(id_vars="index", var_name="var_y", value_name="corr")
        .rename(columns={"index": "var_x"})
    )

# Generate the correlation heatmap (Pearson)

corr_long = corr_to_long(corr_pearson)

heatmap = (
    alt.Chart(corr_long)
    .mark_rect()
    .encode(
        x=alt.X(
            "var_x:O",
            title=None,
            sort=None,
            axis=alt.Axis(labelAngle=-45, labelFontSize=13, titleFontSize=13)
        ),
        y=alt.Y(
            "var_y:O",
            title=None,
            sort=None,
            axis=alt.Axis(labelFontSize=13, titleFontSize=13)
        ),
        color=alt.Color(
            "corr:Q",
            scale=alt.Scale(
                range=["#CC0000", "#F3F0E6", "#1195B2"],
                domain=[-1, 1],
                domainMid=0,
                clamp=True
            ),
            title="Correlation"
        ),
        tooltip=[
            alt.Tooltip("var_x:N", title="X"),
            alt.Tooltip("var_y:N", title="Y"),
            alt.Tooltip("corr:Q", format=".2f", title="ρ"),
        ],
    )
    .properties(
        width=700,
        height=700,
        autosize=alt.AutoSizeParams(type="pad"),
        title="Audio Features - Correlation Heatmap (Pearson)"
    )
)

# auto-detected possible Altair chart: heatmap
try:
    st.altair_chart(heatmap, use_container_width=True)
except Exception:
    st.write(heatmap)

st.markdown("""
Next, we will check for correlation using the Spearman method\\.
""")

# Compute correlation (Spearman)

corr_spearman = df_encoded.corr(method="spearman")
# auto-detected possible Altair chart: corr_spearman
try:
    st.altair_chart(corr_spearman, use_container_width=True)
except Exception:
    st.write(corr_spearman)

# Generate the correlation heatmap (Spearman)

corr_long = corr_to_long(corr_spearman)

heatmap = (
    alt.Chart(corr_long)
    .mark_rect()
    .encode(
        x=alt.X(
            "var_x:O",
            title=None,
            sort=None,
            axis=alt.Axis(labelAngle=-45, labelFontSize=13, titleFontSize=13)
        ),
        y=alt.Y(
            "var_y:O",
            title=None,
            sort=None,
            axis=alt.Axis(labelFontSize=13, titleFontSize=13)
        ),
        color=alt.Color(
            "corr:Q",
            scale=alt.Scale(
                range=["#CC0000", "#F3F0E6", "#1195B2"],
                domain=[-1, 1],
                domainMid=0,
                clamp=True
            ),
            title="Correlation"
        ),
        tooltip=[
            alt.Tooltip("var_x:N", title="X"),
            alt.Tooltip("var_y:N", title="Y"),
            alt.Tooltip("corr:Q", format=".2f", title="ρ"),
        ],
    )
    .properties(
        width=700,
        height=700,
        autosize=alt.AutoSizeParams(type="pad"),
        title="Audio Features - Correlation Heatmap (Spearman)"
    )
)

# auto-detected possible Altair chart: heatmap
try:
    st.altair_chart(heatmap, use_container_width=True)
except Exception:
    st.write(heatmap)

st.markdown("""
Findings: Using either the Pearson or Spearman method, there appears to be almost no correlation between the audio features and a track's play count on Last\\.fm\\. Other factors, such as artist or movie recognition, may be more significant drivers of play counts at the track level\\.
""")

st.markdown("""
We do see from the heatmaps that track happiness is correlated with energy, danceability, instrumentalness, and loudness\\. This seems to confirm the article we referenced in notebook 5\\.4b with respect to energy and danceability factoring into happiness scores\\. It is surprising to see that happiness has a weak correlation with mode, which in music theory is a key driver or whether a song is perceived as happy\\.
""")
