import streamlit as st
import os, sys

st.set_page_config(page_title="Test_elegant_wedding_theme", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# Any visualization using our team_theme
import altair as alt

from utils.viz_theme import enable, sized
enable()    # You need to enable the theme in order for it to work

alt.data_transformers.disable_max_rows()

print("Altair:", alt.__version__)
print("Renderer:", alt.renderers.active)
print("Theme:", alt.themes.active)

# Let's practice making blobs and datasets
from sklearn.datasets import make_blobs, make_moons
import pandas as pd

# Blobs: 4 clusters in 2D

Xb, yb = make_blobs(n_samples = 600, centers = 4, n_features = 2,
                    cluster_std = 1.2, random_state = 27)
df_blobs = pd.DataFrame(Xb, columns = ["x", "y"])
df_blobs["cluster"] = yb.astype(str)

print(df_blobs)

# Let's visualize df_blobs as a scatterplot

scatter = alt.Chart(df_blobs).mark_point().encode(
    x="x:Q",
    y="y:Q",
    color="cluster:N",
    tooltip=["x:Q", "y:Q", "cluster:N"]
).properties(
    title={
        "text": "Blobs: scatter by cluster",
        "subtitle": "Synthetic data generated with scikit-learn"
    }
)

st.write(scatter)

# Stacked bar: count by cluster, stacked by a coarse x-bin (gives each bar segments)
stacked_bar = (
    alt.Chart(df_blobs)
    .mark_bar()
    .encode(
        x=alt.X("cluster:N", title="Cluster"),
        y=alt.Y("count():Q", title="Count"),
        color=alt.Color("x_bin:N", title="x bin"),
        tooltip=[
            "cluster:N",
            "x_bin:N",
            alt.Tooltip("count():Q", title="Count")
        ],
    )
    .transform_calculate(
        # 5 coarse bins across x to create meaningful stacks
        x_bin="floor(datum.x * 1) / 1"  # adjust multiplier (e.g., *0.5, *2) to change bin granularity
    )
    .properties(title={"text": "Blobs: count by cluster (stacked by x bin)", "anchor": "middle"})
)

st.write(stacked_bar)


# Histogram of x, colored by cluster
hist = (
    alt.Chart(df_blobs)
    .mark_bar(opacity=0.8)
    .encode(
        x=alt.X("x:Q", bin=alt.Bin(maxbins=30), title="x (binned)"),
        y=alt.Y("count():Q", title="Count"),
        color=alt.Color("cluster:N", title="Cluster"),
        tooltip=[
            alt.Tooltip("count():Q", title="Count"),
            alt.Tooltip("cluster:N", title="Cluster")
        ]
    )
    .properties(title={"text": "Blobs: histogram of x (by cluster)", "anchor": "middle"})
)

st.write(hist)

# Multi-line chart: within each cluster, sort by x and plot the step-to-step
# change in distance from origin (Δr). This shows how "jumpy" each cluster is.
multi_line = (
    alt.Chart(df_blobs)
    # 1) distance from origin
    .transform_calculate(
        r="sqrt(datum.x*datum.x + datum.y*datum.y)"
    )
    # 2) within each cluster, compute previous r after sorting by x
    .transform_window(
        prev_r="lag(r)",
        sort=[alt.SortField("x", order="ascending")],
        groupby=["cluster"]
    )
    # 3) difference vs previous row (Δr)
    .transform_calculate(
        dr="datum.prev_r == null ? null : datum.r - datum.prev_r"
    )
    # 4) plot lines for each cluster
    .mark_line()
    .encode(
        x=alt.X("x:Q", title="x (sorted within cluster)"),
        y=alt.Y("dr:Q", title="Δ distance from origin (step-to-step)"),
        color=alt.Color("cluster:N", title="Cluster"),
        tooltip=[
            "cluster:N",
            alt.Tooltip("x:Q", format=".3f"),
            alt.Tooltip("y:Q", format=".3f"),
            alt.Tooltip("dr:Q", title="Δr", format=".3f"),
        ],
    )
    .properties(title={"text": "Blobs: step-to-step Δ distance (one line per cluster)", "anchor": "middle"})
)

# auto-detected possible Altair chart: multi_line
try:
    st.altair_chart(multi_line, use_container_width=True)
except Exception:
    st.write(multi_line)

# -------------------------
# 3) Bubble chart: mean x/y per cluster, size by cluster count
# -------------------------
bubble = (
    alt.Chart(df_blobs)
    .transform_aggregate(
        mean_x="mean(x)",
        mean_y="mean(y)",
        n="count()",
        groupby=["cluster"]
    )
    .mark_circle()
    .encode(
        x=alt.X("mean_x:Q", title="Mean x"),
        y=alt.Y("mean_y:Q", title="Mean y"),
        size=alt.Size("n:Q", title="Cluster size"),
        color=alt.Color("cluster:N", title="Cluster"),
        tooltip=["cluster:N", alt.Tooltip("n:Q", title="Count"),
                 alt.Tooltip("mean_x:Q", title="Mean x", format=".3f"),
                 alt.Tooltip("mean_y:Q", title="Mean y", format=".3f")]
    )
    .properties(title={"text": "Blobs: cluster centers (bubble size = count)", "anchor": "middle"})
)

# auto-detected possible Altair chart: bubble
try:
    st.altair_chart(bubble, use_container_width=True)
except Exception:
    st.write(bubble)

# -------------------------
# 4) Density heatmap (2D binning)
# -------------------------
heatmap = (
    alt.Chart(df_blobs)
    .mark_rect()
    .encode(
        x=alt.X("x:Q", bin=alt.Bin(maxbins=40), title="x (binned)"),
        y=alt.Y("y:Q", bin=alt.Bin(maxbins=40), title="y (binned)"),
        color=alt.Color("count():Q", title="Density"),
        tooltip=[alt.Tooltip("count():Q", title="Count in bin")]
    )
    .properties(title={"text": "Blobs: 2D density heatmap", "anchor": "middle"})
)

# auto-detected possible Altair chart: heatmap
try:
    st.altair_chart(heatmap, use_container_width=True)
except Exception:
    st.write(heatmap)

import numpy as np

# Centroids per cluster
centroids = (
    df_blobs.groupby("cluster")[["x", "y"]]
    .mean()
    .rename(columns={"x": "cx", "y": "cy"})
)

# Attach centroid + compute distance
df_blobs2 = df_blobs.merge(centroids, on="cluster", how="left")
df_blobs2["dist_to_centroid"] = np.sqrt((df_blobs2["x"] - df_blobs2["cx"])**2 + (df_blobs2["y"] - df_blobs2["cy"])**2)

# Convert distance -> "density-ish" score in [0,1] (higher = denser / more central)
dmax = df_blobs2["dist_to_centroid"].max()
df_blobs2["density"] = 1 - (df_blobs2["dist_to_centroid"] / dmax)


# Render the chart
chart = alt.Chart(df_blobs2).mark_point(filled=True, size=70).encode(
    x="x:Q",
    y="y:Q",
    color=alt.Color("cluster:N", legend=alt.Legend(title="cluster")),
    opacity=alt.Opacity(
        "density:Q",
        scale=alt.Scale(domain=[0, 1], range=[0.25, 1.0]),
        legend=alt.Legend(title="centrality (opacity)")
    ),
    tooltip=["x:Q", "y:Q", "cluster:N", "density:Q", "dist_to_centroid:Q"]
).properties(
    title={"text": "Blobs: hue by cluster",
           "subtitle": "Saturation (opacity) = closeness to the cluster center"}
)

st.altair_chart(chart, use_container_width=True)
