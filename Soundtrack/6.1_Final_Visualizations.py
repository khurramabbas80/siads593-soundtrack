import streamlit as st
import os, sys

st.set_page_config(page_title="6.1 Final Visualizations", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# I\\. Setup
""")

# Standard library imports
import sys
from datetime import datetime

# Third-party imports
import altair as alt
import numpy as np
import pandas as pd

# os.chdir("/work") # needed for the read_csvs to find files  # path adjusted for Streamlit

# make sure /work is first on the import path (ahead of site-packages)
# sys.path adjusted: add DATA_DIR so local utils/ can be found
if DATA_DIR not in sys.path:
    sys.path.insert(0, DATA_DIR)

pd.set_option('display.max_columns', None)
pd.set_option("display.width", 200)

# Any visualization using our team_theme
import altair as alt

from utils.viz_theme import enable, sized
enable()    # You need to enable the theme in order for it to work

# Important: revert to inlining (this is what worked for you)
alt.data_transformers.enable("default")
alt.data_transformers.disable_max_rows()

# Load the key dataframe

albums_df = pd.read_csv("./pipeline/5.5.Albums_for_final_viz.csv")

st.dataframe(albums_df.head())

print(albums_df.columns.tolist())

st.markdown("""
# II\\. Ridge visualization
""")

st.markdown("""
### II\\.1 Prep the ridge dataframe
""")

st.markdown("""
To better understand which factors are associated with soundtrack popularity, I prepared a ridge plot comparing listener distributions across a small set of carefully chosen features\\. Rather than including everything, I focused on variables that are well populated, interpretable, and conceptually distinct \\(exposure, timing, structure, recognition, and quality\\)\\. Continuous variables were split into simple groups, and award indicators were grouped into recognized vs\\. not recognized\\. This approach allows us to compare distributions directly without assuming linear relationships\\.
""")

st.markdown("""
Cell 1 — Select and organize ridge plot features
""")

st.markdown("""
This cell defines the outcome variable for the ridgeline analysis \\(log album listeners\\) and selects a curated set of features to compare against it\\. Features are grouped into intuitive categories \\(exposure, timing, structure, creator signal, quality, and recognition\\) primarily for readability and team discussion\\. We then combine these feature lists into a single collection, keep only the relevant columns from albums\\_df, and create viz\\_df, the working dataset used in subsequent ridge plot preparation steps\\.
""")

# ============================================================
# CELL 1 — Select features for ridge plot
# ============================================================

# Outcome variable: every ridge compares distributions of this target
y_col = "log_lfm_album_listeners"

# ------------------------------------------------------------
# Final ridge features (chosen to be interpretable + well-populated)
# Grouped for readability; the grouping does not affect analysis logic
# ------------------------------------------------------------

# Exposure / reach signals
cont_features = [
    "film_vote_count",
    "film_popularity",
]

# Timing / recency signal
timing_features = [
    "days_since_album_release",
]

# Content / structure proxies
structure_features = [
    "n_tracks",
    "film_runtime_min",
]

# Creator signal (proxy for composer prolificacy)
creator_features = [
    "composer_album_count",
]

# Quality signal
quality_features = [
    "film_rating",
]

# Recognition signals (counts + BAFTA flag)
award_features = [
    "us_score_nominee_count",
    "us_song_nominee_count",
    "bafta_nominee",
]

# Combine feature groups into a single ordered list
all_features = (
    cont_features
    + timing_features
    + structure_features
    + creator_features
    + quality_features
    + award_features
)

# Keep only columns that actually exist (guards against missing fields)
keep_cols = [y_col] + [c for c in all_features if c in albums_df.columns]

# Working dataframe used throughout ridge plot prep
viz_df = albums_df[keep_cols].copy()

print("viz_df shape (raw):", viz_df.shape)
st.altair_chart(viz_df.head(), use_container_width=True)

st.markdown("""
Cell 2 — Standardize types and remove unusable rows
""")

st.markdown("""
This cell performs lightweight data cleaning so downstream grouping and density calculations behave predictably\\. We coerce the outcome variable \\(log album listeners\\) to numeric and drop rows where it’s missing, since those observations cannot contribute to the ridge distributions\\. We then coerce continuous\\-style features to numeric types, and normalize the awards features into consistent numeric formats: nomination counts become numeric with missing values treated as zero, and boolean flags are converted into 0/1 indicators\\. The result is a cleaned viz\\_df with consistent data types and a reliable outcome column\\.
""")

# ============================================================
# CELL 2 — Clean data types and drop missing outcomes
# ============================================================

# 1) Ensure outcome is numeric
viz_df[y_col] = pd.to_numeric(viz_df[y_col], errors="coerce")

# Drop rows without listener data
viz_df = viz_df.dropna(subset=[y_col])

# 2) Continuous-style features → numeric
for c in cont_features + timing_features + structure_features + creator_features + quality_features:
    if c in viz_df.columns:
        viz_df[c] = pd.to_numeric(viz_df[c], errors="coerce")

# 3) Award features
#    - counts: numeric, NaN → 0
#    - booleans: convert to 0/1
for c in award_features:
    if c in viz_df.columns:
        if "count" in c:
            viz_df[c] = pd.to_numeric(viz_df[c], errors="coerce").fillna(0)
        else:
            viz_df[c] = viz_df[c].fillna(False).astype(bool).astype(int)

print("viz_df shape (after cleaning):", viz_df.shape)
st.altair_chart(viz_df.head(), use_container_width=True)

st.markdown("""
Cell 3 — Create two\\-group splits for each feature
""")

st.markdown("""
This cell creates the binary group labels that power the ridge plots\\. For each feature, we generate a corresponding \\*\\_group column that assigns every album into exactly two interpretable buckets\\. Continuous\\-style features are split at their median into “Above median” vs “Below median\\.” Award count features are simplified into “0 \\(none\\)” vs “1\\+ \\(recognized\\)\\.” Finally, the BAFTA nominee flag is converted into a consistent two\\-level label \\(“True” vs “False”\\)\\. These \\*\\_group columns are used later to compute density curves and to standardize all features into a comparable Yes/No framing\\.
""")

# ============================================================
# CELL 3 — Create group labels (2 groups per feature)
# ============================================================

# 3a) Continuous-style features: split into Above/Below median buckets
cont_like = (
    cont_features
    + timing_features
    + structure_features
    + creator_features
    + quality_features
)

for c in cont_like:
    if c in viz_df.columns:
        # Median split keeps groups balanced and easy to interpret
        median_val = viz_df[c].median(skipna=True)
        viz_df[f"{c}_group"] = np.where(
            viz_df[c] >= median_val,
            "Above median",
            "Below median"
        )

# 3b) Award counts: collapse into "none" vs "any recognition"
for c in award_features:
    if c in viz_df.columns and "count" in c:
        viz_df[f"{c}_group"] = np.where(
            viz_df[c] >= 1,
            "1+ (recognized)",
            "0 (none)"
        )

# 3c) BAFTA nominee flag: standardize to string labels for consistency downstream
if "bafta_nominee" in viz_df.columns:
    viz_df["bafta_nominee_group"] = np.where(
        viz_df["bafta_nominee"] == 1,
        "True",
        "False"
    )

print("Example group columns:")
st.altair_chart(viz_df.filter(regex="_group$").head(), use_container_width=True)

st.markdown("""
Cell 4 — Reshape to ridge\\-ready “long” format
""")

st.markdown("""
This cell converts viz\\_df from a wide, one\\-row\\-per\\-album table into a long format that is easier to use for ridge plot preparation\\. We gather all \\*\\_group columns into two fields: feature \\(which feature the split belongs to\\) and group \\(the album’s assigned bucket for that feature\\)\\. The outcome variable \\(log\\_lfm\\_album\\_listeners\\) is carried along unchanged\\.
""")

st.markdown("""
We also clean the feature names by removing the \\_group suffix, and we optionally set an explicit ordering for the group categories so downstream plots and summaries behave consistently across different feature types\\.
""")

# ============================================================
# CELL 4 — Build ridge-ready dataframe (long format)
# ============================================================

# Collect all binary split columns created in Cell 3
group_cols = [c for c in viz_df.columns if c.endswith("_group")]

# Long format: one row per album-feature pair
# (e.g., the same album appears once for film_rating, once for n_tracks, etc.)
ridge_long = (
    viz_df[[y_col] + group_cols]
    .melt(
        id_vars=[y_col],           # keep outcome fixed
        value_vars=group_cols,     # stack all group columns
        var_name="feature",
        value_name="group"
    )
    .dropna(subset=[y_col, "group"])  # ensure usable rows for plotting
    .copy()
)

# Remove "_group" suffix so feature names match base feature columns / labels
ridge_long["feature"] = ridge_long["feature"].str.replace("_group$", "", regex=True)

# Set a consistent ordering across mixed group label types
# (helps keep legends/facets stable even when some categories are absent)
ridge_long["group"] = pd.Categorical(
    ridge_long["group"],
    categories=[
        "Below median",
        "Above median",
        "0 (none)",
        "1+ (recognized)",
        "False",
        "True",
    ],
    ordered=True
)

print("ridge_long shape:", ridge_long.shape)
st.write(ridge_long.head(10))

print(ridge_long['feature'].unique())

st.markdown("""
Cell 5 — Precompute density curves in pandas \\(plot\\-ready only\\)
""")

st.markdown("""
This cell creates ridge\\_density\\_long, a plot\\-ready dataframe containing the x–y coordinates for each density curve\\. We do this in pandas \\(rather than using Altair’s transform\\_density\\) because Deepnote can struggle with faceted density transforms and large client\\-side Vega\\-Lite computations\\. Precomputing the curves makes the ridgeline chart more reliable and faster to render\\.
""")

st.markdown("""
Conceptually, we take the long\\-format ridge data \\(ridge\\_long\\), and for each \\(feature, group\\) pair we estimate a smooth density curve over a shared x\\-axis grid\\. Using a common x\\-grid ensures curves are directly comparable across features and groups\\. The output is strictly for visualization: it captures the overall distribution shape without changing the underlying listener data used for ordering or summary statistics\\.
""")

# ============================================================
# CELL 5 — Precompute density curves (plot-ready dataframe only)
# ============================================================
# Output:
#   ridge_density_long with columns:
#     feature, group, x, density, n_obs
#
# Why: Deepnote can be unstable/slow with Altair transform_density on many facets,
# so we compute the curves in pandas once and plot the resulting coordinates.

# -----------------------------
# Tunable parameters
# -----------------------------
BINS = 80               # number of x bins per density curve (higher = finer resolution)
SMOOTH_WINDOW = 11      # moving-average window (odd integer); higher = smoother curves

Y_COL = "log_lfm_album_listeners"

# -----------------------------
# Safety checks
# -----------------------------
# Ensure ridge_long has the minimum required columns
need_cols = {Y_COL, "feature", "group"}
missing_cols = [c for c in need_cols if c not in ridge_long.columns]
if missing_cols:
    raise ValueError(f"ridge_long is missing required columns: {missing_cols}")

# Drop unusable rows; everything below assumes complete (feature, group, y) tuples
df = ridge_long.dropna(subset=[Y_COL, "feature", "group"]).copy()

print("ridge_long (clean) shape:", df.shape)
print("n_features:", df["feature"].nunique(), "| n_groups:", df["group"].nunique())

# -----------------------------
# Shared x-grid for ALL curves
# -----------------------------
# Use a common x range so densities are comparable across features/groups
x_min = float(df[Y_COL].min())
x_max = float(df[Y_COL].max())
bin_edges = np.linspace(x_min, x_max, BINS + 1)
bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

# Simple moving-average smoothing kernel for the histogram-based density
w = int(SMOOTH_WINDOW)
if w < 1 or w % 2 == 0:
    raise ValueError("SMOOTH_WINDOW must be a positive odd integer (e.g., 5, 7, 9).")
kernel = np.ones(w) / w

# -----------------------------
# Compute density per (feature, group)
# -----------------------------
rows = []

for (feature, group), g in df.groupby(["feature", "group"]):
    x = g[Y_COL].to_numpy()

    # Skip tiny groups to avoid noisy / misleading density curves
    if len(x) < 10:
        continue

    # Histogram-based density estimate on the shared grid
    dens, _ = np.histogram(x, bins=bin_edges, density=True)

    # Light smoothing to reduce bin-to-bin jaggedness
    dens_smooth = np.convolve(dens, kernel, mode="same")

    # Store plot coordinates + sample size metadata
    rows.append(pd.DataFrame({
        "feature": feature,
        "group": group,
        "x": bin_centers,
        "density": dens_smooth,
        "n_obs": len(x)
    }))

# Concatenate all curves into one plot-ready table
ridge_density_long = pd.concat(rows, ignore_index=True)

print("ridge_density_long shape:", ridge_density_long.shape)
print("features:", ridge_density_long["feature"].nunique(), "| groups:", ridge_density_long["group"].nunique())
st.write(ridge_density_long.head(10))

st.markdown("""
### IV\\.2 Ridge plot of features \\(practice\\)
""")

st.markdown("""
Quick sanity check — single\\-feature density plot
""")

st.markdown("""
Before stacking multiple ridges, this cell plots the precomputed density curves for a single feature \\(film\\_vote\\_count\\)\\. This is a lightweight verification step to confirm that our pandas\\-generated densities look reasonable \\(smooth curves, sensible overlap/separation between groups, no obvious artifacts from binning or smoothing\\)\\. If this single\\-feature plot looks right, the full ridgeline chart is much more likely to render correctly\\.
""")

# Let's do a simple ridge plot on one feature to get familiar wth density plots

one = ridge_density_long[ridge_density_long["feature"] == "film_vote_count"]

density = alt.Chart(one).mark_area(opacity=0.6).encode(
    x=alt.X("x:Q", title="log(album_listeners)"),
    y=alt.Y("density:Q", title="Density"),
    color=alt.Color("group:N", title="Group")
).properties(
    title={"text": "Density check (single feature)", "subtitle": "precomputed in pandas"}
)

# auto-detected possible Altair chart: density
try:
    st.altair_chart(density, use_container_width=True)
except Exception:
    st.write(density)

st.markdown("""
Fantastic\\! the density plots are showing up quite nicely\\. Let's now try building it for multiple features
""")

st.markdown("""
### IV\\.3 Ridge plot of features \\(for real now\\.\\.\\)
""")

st.markdown("""
Step 1 — Build the ridgeline plotting table \\(labels \\+ standardized groups\\)
""")

st.markdown("""
This cell prepares the density output from Cell 5 for use in the final ridgeline chart\\. We start from ridge\\_density\\_long \\(which contains the precomputed x/density coordinates\\) and attach human\\-readable feature labels for display\\. We also standardize the various two\\-group encodings used across features \\(above/below median, 1\\+/0 nominations, true/false\\) into a consistent semantic indicator: “Yes” \\(condition met\\) vs “No” \\(condition not met\\)\\. Finally, we sort the rows so each density curve draws smoothly from left to right when rendered as an area mark\\.
""")

# ============================================================
# STEP 1 — Ridgeline-ready density table
# ============================================================

# Human-readable labels shown on the left side of the final chart
FEATURE_LABELS = {
    "film_vote_count": "Film exposure above median",
    "film_popularity": "Film popularity above median",
    "days_since_album_release": "Album released longer ago than median",
    "composer_album_count": "Composer appears on many albums (above median)",
    "film_rating": "Film rating above median",
    "n_tracks": "Album has many tracks (above median)",
    "film_runtime_min": "Film runtime above median",
    "us_score_nominee_count": "Any score nominations",
    "us_song_nominee_count": "Any song nominations",
    "bafta_nominee": "BAFTA nominee",
}

# ------------------------------------------------------------
# Layout knobs (visual only; the actual ordering is determined later)
# ------------------------------------------------------------
ROW_GAP = 2.5         # Vertical spacing between feature rows
DENSITY_SCALE = 11.0  # Controls ridge "height" (visual emphasis)
LABEL_Y_OFFSET = 0.0  # Fine-tune label vertical alignment (usually 0)

# ------------------------------------------------------------
# Build ridgeline dataframe from precomputed densities
# ------------------------------------------------------------
ridge_ridge_df = ridge_density_long.copy()

# Attach display labels so the chart uses readable feature names
ridge_ridge_df["feature_label"] = ridge_ridge_df["feature"].map(FEATURE_LABELS)

# ------------------------------------------------------------
# Standardize group labels into exactly two semantic conditions
# (we have mixed encodings across features: above/below, 1+/0, true/false)
# ------------------------------------------------------------
def to_high_low(g):
    """
    Normalize all binary group splits into:
      - 'Yes' = condition met (above median / any nomination / True)
      - 'No'  = condition not met
    """
    s = str(g).lower()
    if ("above" in s) or ("1+" in s) or (s == "true"):
        return "Yes"
    return "No"

ridge_ridge_df["group_std"] = ridge_ridge_df["group"].map(to_high_low)

# Sort so each area mark connects smoothly left → right within each curve
ridge_ridge_df = ridge_ridge_df.sort_values(
    ["feature_label", "group_std", "x"]
).reset_index(drop=True)

print("Rows:", ridge_ridge_df.shape)
st.dataframe(ridge_ridge_df.head(5))

import altair as alt

# Sort so areas connect smoothly
ridge_ridge_df = ridge_ridge_df.sort_values(["feature_idx", "group_idx", "x"]).copy()

ridges = alt.Chart(ridge_ridge_df).mark_area(opacity=0.55).encode(
    x=alt.X("x:Q", title="log(album listeners)", scale=alt.Scale(zero=False)),
    y=alt.Y("y1:Q", axis=None),
    y2="y0:Q",
    color=alt.Color("group_std:N", title="Group", sort=["Low", "High"]),
    detail=alt.Detail("feature_label:N"),
    order=alt.Order("x:Q")
).properties(
    width=780,
    height=600,
    title={
        "text": "Listener distributions by feature group",
        "subtitle": "Each row compares two groups; right-shifts indicate higher listener counts are more common in that group."
    }
)

labels_df = (
    ridge_ridge_df[["feature_label", "feature_idx"]]
    .drop_duplicates()
    .sort_values("feature_idx")
    .copy()
)
labels_df["y_label"] = labels_df["feature_idx"] * ROW_GAP + (GROUP_NUDGE / 2.0)

labels = alt.Chart(labels_df).mark_text(align="right", dx=-6).encode(
    x=alt.value(0),
    y=alt.Y("y_label:Q", axis=None),
    text="feature_label:N"
)

baselines = alt.Chart(labels_df).mark_rule(opacity=0.15).encode(
    y="y_label:Q"
)

LEFT_PAD = 220

final_ridge = (baselines + labels + ridges).configure_view(stroke=None).configure(
    padding={"left": LEFT_PAD, "right": 20, "top": 20, "bottom": 20}
)

# auto-detected possible Altair chart: final_ridge
try:
    st.altair_chart(final_ridge, use_container_width=True)
except Exception:
    st.write(final_ridge)

import altair as alt

# Sort so areas connect smoothly
ridge_ridge_df = ridge_ridge_df.sort_values(["feature_idx", "group_idx", "x"]).copy()

ridges = alt.Chart(ridge_ridge_df).mark_area(opacity=0.55).encode(
    x=alt.X("x:Q", title="log(album listeners)", scale=alt.Scale(zero=False)),
    y=alt.Y("y1:Q", axis=None),
    y2="y0:Q",
    color=alt.Color("group_std:N", title="Group", sort=["Low", "High"]),
    detail=alt.Detail("feature_label:N"),
    order=alt.Order("x:Q")
).properties(
    width=780,
    height=600,
    title={
        "text": "Listener distributions by feature group",
        "subtitle": "Each row compares two groups; right-shifts indicate higher listener counts are more common in that group."
    }
)

labels_df = (
    ridge_ridge_df[["feature_label", "feature_idx"]]
    .drop_duplicates()
    .sort_values("feature_idx")
    .copy()
)
labels_df["y_label"] = labels_df["feature_idx"] * ROW_GAP + (GROUP_NUDGE / 2.0)

labels = alt.Chart(labels_df).mark_text(align="right", dx=-6).encode(
    x=alt.value(0),
    y=alt.Y("y_label:Q", axis=None),
    text="feature_label:N"
)

baselines = alt.Chart(labels_df).mark_rule(opacity=0.15).encode(
    y="y_label:Q"
)

LEFT_PAD = 220

final_ridge = (baselines + labels + ridges).configure_view(stroke=None).configure(
    padding={"left": LEFT_PAD, "right": 20, "top": 20, "bottom": 20}
)

# auto-detected possible Altair chart: final_ridge
try:
    st.altair_chart(final_ridge, use_container_width=True)
except Exception:
    st.write(final_ridge)

st.markdown("""
NOTE: Because of a bug in the latest Deepnote notebook update, you are going to see an error "Something went wrong while rendering the block\\. Please refresh the browser or contact our support\\." \\-\\- DO NOT PANIC\\.
""")

st.markdown("""
This error will halt automatic execution of notebook cells at this point onwards\\. However, the next cells will still execute properly \\-\\- you just have to manually run them one\\-by\\-one\\. Don't worry if you are seeing the error again in the cells below \\-\\- it's harmless\\. The cells will still execute\\.
""")

st.markdown("""
Step 1\\.5 — Determine ridge plot ordering
""")

st.markdown("""
This cell computes a data\\-driven ordering of features for the ridge plot\\. Using album\\-level log listener counts from viz\\_df, it compares the median listener values for albums that meet each condition versus those that do not\\. All feature\\-specific group encodings \\(e\\.g\\., above/below median, recognized/not recognized, true/false\\) are normalized to a consistent Yes / No representation before comparison\\.
""")

st.markdown("""
The absolute difference between the Yes and No medians is calculated for each feature, and features are sorted from largest to smallest separation\\. The resulting ordered list of feature labels is used in the subsequent ridge plot to control vertical stacking\\.
""")

# ------------------------------------------------------------
# Median-gap ordering using existing FEATURE_LABELS
#
# This block computes a principled vertical ordering for the
# ridge plot based on album-level listener data.
#
# For each binary feature condition (e.g., above/below median,
# nominee vs non-nominee), we compare the median log album
# listeners for albums that meet the condition ("Yes") versus
# those that do not ("No"). The absolute difference between
# these medians is used as a simple measure of separation
# between the two groups.
#
# The resulting feature ordering is later used to stack ridge
# plots from strongest to weakest separation.
# ------------------------------------------------------------

# Build the list of *_group columns corresponding to the
# human-readable feature labels used in the visualization
group_cols = [f"{c}_group" for c in FEATURE_LABELS.keys()]

# Reshape album-level data to long format:
# one row per album per feature condition
order_long = (
    viz_df[["log_lfm_album_listeners"] + group_cols]
    .melt(
        id_vars="log_lfm_album_listeners",
        var_name="feature_group_col",
        value_name="group_raw"
    )
    .assign(
        # Recover the base feature name from the *_group column
        # and map it to the human-readable label used in the plot
        feature=lambda d: d["feature_group_col"].str.replace("_group$", "", regex=True),
        feature_label=lambda d: d["feature"].map(FEATURE_LABELS),

        # Normalize heterogeneous group encodings
        # (Above/Below median, 1+/0 recognized, True/False)
        # into a consistent Yes / No indicator
        group_std=lambda d: d["group_raw"].apply(
    lambda v: "Yes" if v in ["Above median", "1+ (recognized)", True, "True", 1] else "No")
    )
)

# Compute median log listeners for Yes and No groups
# and rank features by the absolute median difference
median_gap = (
    order_long
    .groupby(["feature_label", "group_std"])["log_lfm_album_listeners"]
    .median()
    .unstack("group_std")
    .assign(median_gap=lambda d: (d["Yes"] - d["No"]).abs())
    .sort_values("median_gap", ascending=False)
)

# Extract the ordered feature labels for use in the ridge plot
feature_labels_ordered = median_gap.index.tolist()

# auto-detected possible Altair chart: median_gap
try:
    st.altair_chart(median_gap, use_container_width=True)
except Exception:
    st.write(median_gap)

st.markdown("""
Step 1\\.6 — Apply ridge layout order
""")

st.markdown("""
This cell applies the feature ordering determined in Step 1\\.5 to the ridgeline geometry\\. At this point, the desired vertical order of features has already been computed based on median separation between the “Yes” and “No” groups; here, we translate that ordering into actual y\\-coordinates used by the chart\\.
""")

st.markdown("""
Each feature label is mapped to a new vertical index so that features with the largest separation appear at the top of the plot and those with weaker separation appear lower\\. Using this index, we compute the baseline \\(y0\\) for each ridge and the upper boundary \\(y1\\) by scaling the density values\\. The vertical spacing between ridges and their visual height are controlled by the layout constants defined earlier\\.
""")

st.markdown("""
Finally, the dataframe is sorted to ensure that density curves are drawn smoothly from left to right within each ridge\\. From this point onward, the ridgeline dataframe reflects the final visual layout used in the chart\\.
""")

# ============================================================
# STEP 1.6 — Apply ridge layout order (y0/y1) after ordering
# ============================================================

# Map feature_label -> new vertical index based on median-gap ordering
# Put the strongest separation at the TOP (highest y)
n = len(feature_labels_ordered)
feature_label_to_idx = {fl: (n - 1 - i) for i, fl in enumerate(feature_labels_ordered)}

ridge_ridge_df["feature_idx"] = ridge_ridge_df["feature_label"].map(feature_label_to_idx)

# Compute vertical coordinates
ridge_ridge_df["y0"] = ridge_ridge_df["feature_idx"] * ROW_GAP
ridge_ridge_df["y1"] = ridge_ridge_df["y0"] + ridge_ridge_df["density"] * DENSITY_SCALE

# Final sort for smooth drawing
ridge_ridge_df = ridge_ridge_df.sort_values(
    ["feature_idx", "group_std", "x"]
).reset_index(drop=True)

st.markdown("""
Step 2 — Build the ridgeline layers \\(fill \\+ outline\\)
""")

st.markdown("""
This cell constructs the ridgeline visualization from the precomputed density table \\(ridge\\_ridge\\_df\\)\\. Features are ordered using the median\\-separation ranking computed in Step 1\\.5, so that the most separated Yes/No distributions appear most prominently\\.
""")

st.markdown("""
We build a shared “base” Altair chart that defines the common encodings \\(x\\-axis, vertical ridge coordinates, and color mapping for Yes/No\\)\\. Then, for each feature, we create a two\\-layer ridge: a semi\\-transparent filled area plus a black outline drawn from the same density curve\\. Finally, all feature\\-specific ridges are stacked into a single layered chart with a shared title and subtitle\\.
""")

# ============================================================
# STEP 2 — Ridges layer
# ============================================================
ridge_ridge_df["group_order"] = ridge_ridge_df["group_std"].map({"No": 0, "Yes": 1})


# ============================================================
# STEP 2 — Ridges layer (fill + outline as two layers)
# ============================================================

# 1) Fill layer (no stroke)
ridges_fill = alt.Chart(ridge_ridge_df).mark_area(
    fillOpacity=0.8,   # make fill obvious
    stroke=None
).encode(
    x=alt.X("x:Q", title="log(album listeners)", scale=alt.Scale(zero=False)),
    y=alt.Y("y1:Q", axis=None),
    y2="y0:Q",
    color=alt.Color(
        "group_std:N",
        sort=["No", "Yes"],
        legend=alt.Legend(title="Condition met")
    ),
    detail="feature_label:N",
    order="x:Q"
)

# 2) Outline layer (line only)
ridges_line = alt.Chart(ridge_ridge_df).mark_line(
    strokeWidth=2
).encode(
    x="x:Q",
    y="y1:Q",
    color=alt.Color("group_std:N", sort=["No", "Yes"], legend=None),
    detail="feature_label:N",
    order="x:Q"
)

# Combine
ridges = (ridges_line + ridges_fill).properties(
    width=780,
    height=600
)

# auto-detected possible Altair chart: ridges
try:
    st.altair_chart(ridges, use_container_width=True)
except Exception:
    st.write(ridges)


# ============================================================
# STEP 2 — Ridges layer (fill + outline as two layers)
# ============================================================

# Use Step 1.5 ranking (largest median gap first) to determine draw order
feature_labels_ordered = (
    median_gap["median_gap"]
    .sort_values(ascending=False)
    .index
    .tolist()
)

# Shared encodings for all ridges; y0/y1 were computed in Step 1.6
base = alt.Chart(ridge_ridge_df).encode(
    x=alt.X("x:Q", title="log(album listeners)", scale=alt.Scale(zero=False)),
    y=alt.Y("y1:Q", axis=None),   # ridge top
    y2="y0:Q",                    # ridge baseline
    color=alt.Color(
        "group_std:N",
        sort=["No", "Yes"],
        legend=alt.Legend(title="Condition met")
    )
)

# Build one ridge per feature as a 2-layer stack:
# (1) filled density + (2) outline for legibility
layers = [
    alt.layer(
        base.transform_filter(alt.datum.feature_label == fl).mark_area(
            fillOpacity=0.6,
            stroke=None
        ),
        base.transform_filter(alt.datum.feature_label == fl).mark_area(
            fillOpacity=0,
            stroke="black",
            strokeWidth=1.5
        )
    )
    for fl in feature_labels_ordered
]

# Combine all feature ridges into a single layered chart
ridges = alt.layer(*layers).properties(
    width=780,
    height=600,
    title={
        "text": "Listener distributions by feature group",
        "subtitle": [
            "The clearest right-shifts appear for film exposure, soundtrack recognition (nominations/BAFTA),",
            "and longer time since album release; other conditions show substantial overlap between groups."
        ]
    }
)

# auto-detected possible Altair chart: ridges
try:
    st.altair_chart(ridges, use_container_width=True)
except Exception:
    st.write(ridges)

st.markdown("""
Step 3 — Prepare the label positions
""")

st.markdown("""
This cell builds a small dataframe used exclusively for left\\-side feature labels \\(and optional baseline rules\\)\\. We pull one \\(feature\\_label, y0\\) pair per feature from the ridgeline density table, then explicitly reorder the rows to match feature\\_labels\\_ordered so labels stay aligned with the stacked ridges\\. Finally, we compute y\\_label, which applies a small vertical offset to fine\\-tune label alignment relative to each ridge baseline\\.
""")

# ============================================================
# STEP 3 — Build labels dataframe
# ============================================================

# One label per feature: grab each feature's baseline y0 from the ridge table
labels_df = (
    ridge_ridge_df[["feature_label", "y0"]]
    .drop_duplicates()                # collapse to unique feature rows
    .set_index("feature_label")       # index enables easy reordering via .loc
    .loc[feature_labels_ordered]      # keep label order consistent with ridge order
    .reset_index()
    .copy()
)

# Optionally nudge label vertical placement (usually 0 unless tuning alignment)
labels_df["y_label"] = labels_df["y0"] + LABEL_Y_OFFSET

st.dataframe(labels_df)

baselines = alt.Chart(labels_df).mark_rule(opacity=0.15).encode(y="y_label:Q")
combo_2 = (baselines + labels_only + ridges_only)
# auto-detected possible Altair chart: combo_2
try:
    st.altair_chart(combo_2, use_container_width=True)
except Exception:
    st.write(combo_2)

st.markdown("""
STEP 4 — Assemble the final ridgeline chart
""")

st.markdown("""
This cell composes the final visualization by combining three elements: the ridgeline density layers \\(ridges\\), a left\\-side text label layer \\(labels\\), and faint horizontal baseline guides \\(baselines\\)\\. We also apply final layout tweaks \\(removing the plot border and adding left padding\\) so labels have room and the chart reads cleanly as a stacked, annotated figure\\.
""")

# ============================================================
# STEP 4 — Final ridgeline chart
# ============================================================

labels = alt.Chart(labels_df).mark_text(
    align="right",
    baseline="middle",
    dx=-8
).encode(
    x=alt.value(0),
    y=alt.Y("y_label:Q", axis=None),
    text="feature_label:N"
)

baselines = alt.Chart(labels_df).mark_rule(
    opacity=0.15
).encode(
    y="y_label:Q"
)

final_ridge = (labels + ridges + baselines).configure_view(
    stroke=None
).configure(
    padding={"left": 260, "right": 20, "top": 20, "bottom": 20}
)


# auto-detected possible Altair chart: final_ridge
try:
    st.altair_chart(final_ridge, use_container_width=True)
except Exception:
    st.write(final_ridge)

st.markdown("""
Findings: This chart compares the distribution of soundtrack listener counts \\(on a log scale\\) for albums that do versus do not meet each condition\\. Visually, the strongest and most consistent right\\-shifts appear for film exposure \\(vote counts\\) above median, soundtrack recognition \\(any score nominations, any song nominations, and BAFTA nominee\\), and albums released longer ago than the median, where higher listener counts are more common in the “Yes” group\\. 
""")

st.markdown("""
For the remaining features—such as film popularity, composer visibility, film rating, album length, and runtime—the “Yes” and “No” distributions overlap heavily, making it difficult to see a clear separation in this view\\. Overall, this suggests that a small number of exposure\\- and recognition\\-related factors stand out visually, while many commonly cited attributes show only subtle differences when looking at the full distribution of listeners\\.
""")
