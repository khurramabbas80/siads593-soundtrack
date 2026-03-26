import streamlit as st
import os, sys

st.set_page_config(page_title="5.3 Album-track popularity analysis", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

import pandas as pd
import numpy as np
import os
import holoviews as hv
import panel as pn

from bokeh.io import output_file, save
from bokeh.io import output_notebook
from bokeh.resources import INLINE

output_notebook(resources=INLINE)
hv.extension("bokeh")
pn.extension()

# Any visualization using our team_theme
import altair as alt

from utils.viz_theme import enable, sized
enable()    # You need to enable the theme in order for it to work

alt.data_transformers.disable_max_rows()

st.markdown("""
# Overview
""")

st.markdown("""
This notebook focuses on how popularity behaves within and across soundtrack albums, using log\\-transformed listener and playcount metrics\\. The scope is intentionally narrow: all analysis stays within the album–track relationship and avoids introducing external metadata or explanatory variables\\.  
""")

st.markdown("""
The goal here is not to explain why certain soundtracks succeed, but to understand how popularity is distributed, concentrated, and structured once it exists\\.
""")

st.markdown("""
Each section below addresses a specific structural question and is paired with a clear, single visualization\\.
""")

# Load the dataframes
albums_df = pd.read_csv("./pipeline/4.7.Albums_analytics_set.csv")
wide_df = pd.read_csv("./pipeline/4.7.Wide_analytics_set.csv")

print(albums_df.columns.tolist())

st.markdown("""
# I\\. Album popularity distribution
""")

st.markdown("""
We examine how soundtrack popularity is distributed at the album level to understand what “typical” versus “blockbuster” soundtracks look like, and to establish the overall skew of listening across albums\\.
""")

st.markdown("""
Question: How is listening distributed across soundtrack albums?
Visualization: Side\\-by\\-side boxplot and violin plot of log album listeners\\.
""")

source = albums_df[['log_lfm_album_listeners']]

# Boxplot in Altair
c1 = alt.Chart(source).mark_boxplot(size=50).encode(
    y = alt.Y("log_lfm_album_listeners:Q", title = "Log(album listeners)")
).properties(
    width = 160,
    title={
        "text": "Album popularity at a glance",
        "subtitle": "Most albums cluster in a modest range, with substantial overall spread",
        "subtitleFontSize": 11
    }
)

# Violinplot in Altair
c2 = alt.Chart(source).transform_density(
    "log_lfm_album_listeners",
    as_ = ["log_lfm_album_listeners", "density"]
).mark_area(orient = "horizontal").encode(
    y = alt.Y("log_lfm_album_listeners:Q", title = None),
    x = alt.X("density:Q", stack = "center", title = None,
    axis=alt.Axis(labels=False, ticks=False, domain=False))
).properties(
    title={
        "text": "Distribution of soundtrack album popularity",
        "subtitle": "Most albums concentrate in the middle, with a long tail of blockbusters.",
        "subtitleFontSize": 11
    }
)

(c1 | c2).resolve_scale(y="shared")

st.markdown("""
Findings\\. Album popularity is strongly right\\-skewed: most soundtrack albums cluster in a relatively modest range of listener counts, while a small number of albums form a long tail of very high popularity\\. The violin plot highlights where the bulk of albums concentrate, and the boxplot confirms a wide overall spread, reinforcing the need to use log\\-scale popularity in subsequent analyses\\.
""")

st.markdown("""
# II\\. Album Popularity by Musical Style
""")

st.markdown("""
We explore whether soundtrack albums associated with different musical styles exhibit distinct popularity distributions, using genre flags rather than text\\-based tags\\.
""")

st.markdown("""
Question: Do soundtrack albums associated with different musical styles show different popularity profiles?
Visualization: Small\\-multiple boxplots of log album listeners by genre flag \\(e\\.g\\., pop, electronic, classical/orchestral\\)\\.
""")

st.markdown("""
### II\\.1 Faceted boxplots
""")

st.markdown("""
Question: Do soundtrack albums tagged with different musical genres show meaningfully different listener popularity distributions on Last\\.fm?
""")

st.markdown("""
This block converts our genre flags into a tidy “album–genre” table so we can compare popularity distributions by musical style\\. We melt\\(\\) the seven boolean genre columns into a single genre field, keep only the genres an album actually has, and then plot boxplots of log\\(album listeners\\) to compare typical popularity and spread across genres\\.
""")

GENRE_FLAGS = ['ambient_experimental', 'classical_orchestral', 'electronic', 'hip_hop_rnb',
'pop', 'rock', 'world_folk']

# -------------------------------------------------------------------
# Reshape genre flags from "wide" to "long" format using pandas.melt()
#
# Why we do this:
# - In albums_df, genres are represented as 7 separate binary columns
#   (e.g., pop, rock, electronic), and an album can belong to multiple genres.
# - Altair works best with "tidy"/long data where a single categorical column
#   (genre) drives grouping/encoding (e.g., one boxplot per genre).
#
# What melt() does here:
# - id_vars: columns we KEEP as identifiers on every row (album metadata + outcome).
# - value_vars: the genre-flag columns we want to UNPIVOT into rows.
# - var_name: name of the new column that will store the original flag column names
#   (e.g., "pop", "rock", ...).
# - value_name: name of the new column that will store the flag values (0/1).
#
# Resulting structure (long/tidy):
# - One row per (album, genre_flag) pair.
# - Columns include:
#     release_group_id, album_title, log_lfm_album_listeners, genre, has_genre
#
# Handling multi-genre albums:
# - After melting, an album will appear once for EACH genre flag column.
# - We then filter to has_genre == 1 so each album contributes to every genre
#   it is tagged with (non-exclusive membership), which avoids forcing a single
#   "primary" genre assignment.
# -------------------------------------------------------------------
genre_long_df = albums_df.melt(
    id_vars = ['release_group_id', 'album_title', 'log_lfm_album_listeners', 'log_lfm_album_playcount',
    'lfm_album_listeners', 'lfm_album_playcount'],
    value_vars = GENRE_FLAGS,
    var_name = 'genre',
    value_name = 'has_genre'
)

genre_long_df = genre_long_df[genre_long_df['has_genre'] == 1].copy()

st.dataframe(genre_long_df.head())

c1 = alt.Chart(genre_long_df).mark_boxplot(size = 20).encode(
    x = alt.X(
        "genre:N",
        title = None,
        sort = GENRE_FLAGS,
        axis = alt.Axis(labelAngle = -45)
    ),
    y = alt.Y("log_lfm_album_listeners:Q", title = 'Log(album listeners)'),
).properties(
    width = 280,
    title={
        "text": "Album popularity by musical style",
        "subtitle": "Comparing typical popularity and overall spread across genres.",
        "subtitleFontSize": 11
    }
)

# auto-detected possible Altair chart: c1
try:
    st.altair_chart(c1, use_container_width=True)
except Exception:
    st.write(c1)

st.markdown("""
Findings\\. Album popularity varies meaningfully by musical style in terms of spread and extremes, even when median levels are similar\\. Rock and electronic soundtracks exhibit wide interquartile ranges, indicating high variability among typical albums, while hip hop/R&B, classical/orchestral, and ambient/experimental albums cluster more tightly\\. Blockbuster outliers are most prominent in pop and rock, where a small number of albums substantially exceed the typical popularity range\\.
""")

st.dataframe(genre_long_df.groupby('genre').count())

st.markdown("""
### II\\.2 Faceted violinplots
""")

st.markdown("""
Question: For each genre, what’s a high\\-end ‘typical best case’ popularity level \\(the 75th percentile / Q3 of log album listeners\\), so we can rank/label genres accordingly?
""")

st.markdown("""
To make the genre comparison more interpretable than a simple boxplot, we switch to a density \\(“violin”\\) view and add a lightweight ranking signal\\. First, we compute a per\\-genre Q3 \\(75th percentile\\) of log\\(album listeners\\) and the sample size, then embed Q3 directly into the facet labels \\(since adding reference lines is awkward with transform\\_density\\)\\. Finally, we build faceted violin plots—one panel per genre—sorted by Q3 so the highest\\-performing genres appear first while all panels share the same y\\-axis for apples\\-to\\-apples comparison\\.
""")

# 0) Build a real lookup table in pandas (one row per genre)
q3_df = (
    genre_long_df.groupby("genre", as_index=False)
    .agg(
        genre_q3=("log_lfm_album_listeners", lambda s: s.quantile(0.75)),
        genre_n=("log_lfm_album_listeners", "size")
    )
)


# Build label map: {"rock":"rock (Q3=7.87, n=84)", ...}
label_map = {
    r["genre"]: f"{r['genre']} (Q3={r['genre_q3']:.1f})"
    for _, r in q3_df.iterrows()
}

# Turn into a string mapping dictionary:
# {"rock":"rock (Q3=7.87, n=58)","pop":"pop (Q3=6.30, n=84)" ...}
label_expr_obj = "{" + ",".join(
    [f'"{k}":"{v}"' for k, v in label_map.items()]
) + "}"

# Unfortunately, there's no easy way to layer a mark_rule into the violinplot because the
# transform_density changes the x-axis. So let's build the Q3 values right into the genre labels
st.dataframe(q3_df)
st.write(label_expr_obj)


c2 = alt.Chart(genre_long_df).transform_density(
    # Compute per-genre kernel density estimates of log album listeners (violin shapes)
    "log_lfm_album_listeners",
    groupby=['genre'],
    as_=['log_lfm_album_listeners', 'density']
).transform_lookup(
    # Re-attach per-genre Q3 and count (precomputed in pandas) for sorting and tooltips
    lookup="genre",
    from_=alt.LookupData(q3_df, "genre", ["genre_q3", "genre_n"])
).mark_area(
    orient='horizontal',
    opacity=0.7
).encode(
    x=alt.X(
        "density:Q",
        stack="center",
        title=None,
        axis=alt.Axis(labels=False, ticks=False, domain=False),
    ),
    y=alt.Y("log_lfm_album_listeners:Q", title="Log(album listeners)"),
    tooltip=[
        alt.Tooltip("genre:N"),
        alt.Tooltip("genre_q3:Q", title="3rd quartile (log listeners)", format=".2f"),
        alt.Tooltip("genre_n:Q", title="count")
    ],
).properties(width=110).facet(
    # Facet into one panel per genre and sort panels by the genre-level Q3 statistic
    column=alt.Column(
        "genre:N",
        title=None,
        sort=alt.SortField(field="genre_q3", order="descending"),
        header=alt.Header(
            labelOrient="bottom",
            labelExpr=f"{label_expr_obj}[datum.value]"
        )
    )
).resolve_scale(
    # Force a shared y-axis across facets so listener distributions are directly comparable
    y="shared"
).properties(
    title={
        "text": "Album popularity distributions by genre",
        "subtitle": "Density view highlighting overlap and tail behavior across musical styles."
    }
)


# auto-detected possible Altair chart: c2
try:
    st.altair_chart(c2, use_container_width=True)
except Exception:
    st.write(c2)

st.markdown("""
Findings\\. Although album popularity distributions overlap substantially across genres, ordering the violins by the 75th percentile \\(Q3\\) clarifies differences in how frequently higher\\-performing albums appear\\. Genres such as rock, world\\_folk, and electronic sit highest on this scale, indicating that a larger share of albums in these genres reach relatively strong popularity levels rather than relying on a few isolated hits\\. In contrast, hip\\_hop\\_rnb and pop exhibit lower upper\\-quartile values, suggesting that while standout albums exist, high listener counts are less typical across the catalog\\.
""")

st.markdown("""
### II\\.3 Genre scatterplot
""")

st.markdown("""
Question: Do different soundtrack genres show distinct patterns in engagement—i\\.e\\., do some genres cluster into higher \\(or lower\\) listener/playcount territory on Last\\.fm?
""")

st.markdown("""
This scatterplot compares two complementary popularity signals—album listeners \\(reach\\) and album playcount \\(repeat consumption\\)—and colors each album by its mapped genre\\. If genre strongly differentiated popularity, we’d expect visible clusters or separation by color; instead, this view makes it easy to see overlap, outliers, and whether any genre consistently “lives” in a different engagement band\\.
""")

alt.Chart(genre_long_df).mark_circle(
    opacity=0.35,
    size=40
).encode(
    x=alt.X("log_lfm_album_listeners:Q", title="log(album listeners)"),
    y=alt.Y("log_lfm_album_playcount:Q", title="log(album playcount)"),
    color=alt.Color(
        "genre:N",
        title="Genre"
    ),
    tooltip=[
        alt.Tooltip("album_title:N", title="Album"),
        alt.Tooltip("genre:N", title="Genre"),
        alt.Tooltip("lfm_album_listeners:Q", title="Album listeners", format=","),
        alt.Tooltip("lfm_album_playcount:Q", title="Album playcount", format=","),
    ]
).properties(
    width=750,
    height=400,
    title={
        "text": "Popularity by genre",
        "subtitle": "Album popularity does not separate cleanly by genre."
    }
)

st.markdown("""
Findings\\. No clear genre\\-based clustering is visible in album listener–playcount space\\. Albums across musical styles occupy the same popularity continuum, indicating that genre alone does not structure album\\-level engagement patterns\\.
""")

st.markdown("""
### II\\.4 Genre co\\-occurrence with chord diagrams
""")

st.markdown("""
Question: Which soundtrack genres tend to be tagged together on the same album—and how strong are those pairings?
""")

st.markdown("""
This section builds a genre co\\-occurrence view to capture multi\\-genre overlap that gets lost in “one genre at a time” charts\\. We compute how often each pair of genre flags appears together across albums, filter out one\\-off/noisy pairings with a minimum\\-count threshold, and render the strongest relationships as a chord diagram where thicker ribbons indicate more frequent co\\-tagging\\.
""")

# -------------------------------------------------------------------
# GENRE CO-OCCURRENCE CHORD DIAGRAM (HoloViews)
#
# Goal:
#   Visualize how often soundtrack albums are tagged with *pairs* of genres.
#   Each ribbon connects two genres; ribbon thickness = # albums tagged with both.
#
# Data assumptions:
#   - albums_df has 7 binary genre-flag columns in GENRE_FLAGS (0/1).
#   - genre_colors maps each genre name -> hex color (your brand palette).
# -------------------------------------------------------------------

# 1) Normalize genre flags to clean 0/1 integers
#    (Some pipelines store flags as objects/strings; we force consistent ints.)
G = albums_df[GENRE_FLAGS].fillna(0).astype(int)

# 2) Build a 7x7 co-occurrence matrix
#    - G is (n_albums x 7)
#    - G.T @ G yields (7 x 7) where C[a,b] = count of albums where both a==1 and b==1
#    - Diagonal C[a,a] = count of albums that have genre a
C = G.T @ G  # 7x7 DataFrame

# 3) Convert the co-occurrence matrix into an "edge list" for the chord diagram
#    We only keep:
#      - the upper triangle (to avoid duplicate edges a->b and b->a)
#      - non-zero co-occurrences
#    Each edge is: (source_genre, target_genre, cooccurrence_count)
edges = []
for i, a in enumerate(GENRE_FLAGS):
    for j, b in enumerate(GENRE_FLAGS):
        if j <= i:
            continue  # skip diagonal + lower triangle
        cnt = int(C.loc[a, b])
        if cnt > 0:
            edges.append((a, b, cnt))

edges_df = pd.DataFrame(edges, columns=["source", "target", "value"])

# 4) Optional clutter reduction: drop weak/rare co-occurrences
#    MIN_COUNT controls density:
#      - smaller value shows more (potentially noisy) connections
#      - larger value shows only strong, common pairings
MIN_COUNT = 2
edges_df = edges_df[edges_df["value"] >= MIN_COUNT].copy()

# Quick QA: top co-occurring genre pairs
edges_df.sort_values("value", ascending=False).head(10)

genre_colors = {
    "ambient_experimental": "#7922CC",  # purple
    "classical_orchestral": "#1195B2",  # blue/teal (keep one teal)
    "electronic":           "#CC0000",  # red
    "hip_hop_rnb":          "#CE7E00",  # orange
    "pop":                  "#1F6F5B",  # green (distinct from teal)
    "rock":                 "#3F1D5C",  # deep purple (distinct enough from #7922CC)
    "world_folk":           "#8C4A00",  # brown (distinct from orange/red)
}

# 5) Build the node table (the genres around the circle)
#    Important detail:
#      After filtering by MIN_COUNT, a genre might have zero remaining edges.
#      If we include it anyway, some renderers can error or misalign labels.
#      So we only include genres that still appear in edges_df.
used_genres = sorted(set(edges_df["source"]).union(edges_df["target"]))
nodes = pd.DataFrame({"name": used_genres})

# Assign each node a fixed brand color (used for the outer ring / node marker)
nodes["color"] = nodes["name"].map(genre_colors)

# 6) Add an edge color so ribbons are easier to read
#    Here: color each ribbon by its "source" genre color.
#    (This is arbitrary because co-occurrence is undirected, but visually helpful.)
edges_col = edges_df.copy()
edges_col["color"] = edges_col["source"].map(genre_colors)

# 7) Render the chord diagram with HoloViews (Bokeh backend)
#    Reading the chart:
#      - Node labels around the ring = genres
#      - Ribbon thickness = co-occurrence count ("value")
#      - Ribbon color = source genre (to help trace bundles)
chord = hv.Chord((edges_col, hv.Dataset(nodes, "name"))).opts(
    width=800, height=800,
    labels="name",
    node_color="color",            # explicit hex colors for nodes
    edge_color="color",            # color ribbons by source genre
    edge_alpha=0.25,               # lower alpha reduces "blob" overlap
    edge_line_width=hv.dim("value") / edges_col["value"].max() * 8,  # thickness scaling
    node_size=16,
    title=f"Genre co-occurrence across soundtrack albums (≥ {MIN_COUNT} albums)"
)

# auto-detected possible Altair chart: chord
try:
    st.altair_chart(chord, use_container_width=True)
except Exception:
    st.write(chord)

st.markdown("""
Findings: Soundtrack albums are structurally multi\\-genre, with especially strong overlap among contemporary styles \\(electronic, pop, rock\\), and meaningful cross\\-pollination between traditional and modern genres\\.
""")

st.markdown("""
Warning: Note that I've gotten Bokeh to render within Deepnote only a few times, but it is very finicky\\. If the cell above does not render the chord diagram, then the next best option is to have Bokeh render it in \\.html, which you can download and open in a browser\\.
""")

p = hv.render(chord, backend="bokeh")

output_file("genre_chord.html")
save(p)
print("Wrote genre_chord.html")

st.markdown("""
The \\.html would look something like this\\.
""")

st.markdown("""
<img src="image-20260203-004414.png" width="" align="" />
""")

st.markdown("""
# IV\\. Composer Footprint in Album Popularity Space
""")

st.markdown("""
We summarize album popularity at the composer level to explore how composers differ in terms of both output volume and typical soundtrack popularity, without ranking or clustering\\.
""")

st.markdown("""
Question: Do composers differ more by how many soundtrack albums they release, or by how popular those albums tend to be?
Visualization: Scatterplot with x = number of soundtrack albums per composer and y = median log album listeners per composer\\.
""")

print(albums_df['composer_primary_clean'].nunique())
print(albums_df['composer_primary_clean'].value_counts().index.tolist())
print(albums_df['composer_primary_clean'].value_counts().tolist())

st.markdown("""
### IV\\.1 Stacked bar chart of top composers
""")

st.markdown("""
Question: Among the most\\-listened composers in our dataset, is total listener reach driven by a broad catalog—or by one \\(or a few\\) breakout soundtracks?
""")

st.markdown("""
This block focuses on composer\\-level concentration\\. After filtering out unknown composers, we identify the top 15 composers by total album listeners, rank each composer’s albums by listeners, and visualize the totals as stacked bars\\. The stacking highlights whether a composer’s “total” is spread across many albums or dominated by a single standout soundtrack\\.
""")

composer_df = albums_df[(albums_df['composer_primary_clean'] != 'Unknown') & (albums_df['composer_primary_clean'].notna())]

# display(len(albums_df))
# display(len(composer_df))   # 1551 --> 1526, 25 albums removed

# -----------------------------
# 0) Basic cleaning / guardrails
# -----------------------------

# Ensure year exists for tooltip (use US release year if available, else film year)
composer_df["album_year_for_tt"] = composer_df["album_us_release_year"]
composer_df.loc[composer_df["album_year_for_tt"].isna(), "album_year_for_tt"] = composer_df["film_year"]

# -----------------------------
# 1) Pick the "top 15 composers"
#    (by total listeners across albums)
# -----------------------------
top_composers = (
    composer_df.groupby("composer_primary_clean", dropna=False)["lfm_album_listeners"]
      .sum()
      .sort_values(ascending=False)
      .head(15)
      .index
      .tolist()
)

df_top = composer_df[composer_df["composer_primary_clean"].isin(top_composers)].copy()

# ------------------------------------------------------------
# 1b) Rank albums within each composer by album listeners (desc)
#     rank=1 means "top album for this composer"
# ------------------------------------------------------------
df_top["album_rank_in_composer"] = (
    df_top.groupby("composer_primary_clean")["lfm_album_listeners"]
    .rank(method="first", ascending=False)
    .astype(int)
)

TOP_K = 10  # or 8, 12, etc.

df_top["rank_bucket"] = df_top["album_rank_in_composer"].where(
    df_top["album_rank_in_composer"] <= TOP_K,
    TOP_K + 1
)

df_top["rank_bucket_label"] = df_top["rank_bucket"].map(
    lambda r: f"#{r}" if r <= TOP_K else f"#{TOP_K+1}+"
)

# -----------------------------
# 2) Build stacked bar chart
#    - x: label
#    - y: listeners
#    - stacks: album_title, with largest albums at the base (order=desc)
#    - tooltip: album title, year, raw listeners
# -----------------------------
alt.data_transformers.disable_max_rows()

chart = (
    alt.Chart(df_top)
    .mark_bar()
    .encode(
        x=alt.X(
            "composer_primary_clean:N",
            sort=top_composers,                  # keep composers ordered by total listeners
            title="Composer",
            axis = alt.Axis(labelAngle = -45)
        ),
        y=alt.Y(
            "sum(lfm_album_listeners):Q",
            title="Album listeners (sum)"
        ),
        color=alt.Color(    # Make sure all the albums of the same rank within a composer have the same color
            "rank_bucket_label:N",
            title="Album rank within composer",
            sort=[f"#{i}" for i in range(1, TOP_K+1)] + [f"#{TOP_K+1}+"],
            legend=alt.Legend(orient="right")
        ),
        # This controls the stacking order: biggest albums at the base
        order=alt.Order("lfm_album_listeners:Q", sort="descending"),
        tooltip=[
            alt.Tooltip("composer_primary_clean:N", title="Composer"),
            alt.Tooltip("album_title:N", title="Album"),
            alt.Tooltip("rank_bucket_label:N", title="Rank"),
            alt.Tooltip("album_year_for_tt:Q", title="Year", format=".0f"),
            alt.Tooltip("lfm_album_listeners:Q", title="Listeners", format=",.0f"),
        ],
    )
    .properties(
        width=750,
        height=400,
        title={
            'text': "Top 15 Composers — Total Album Listeners (stacked by album)",
            'subtitle': ["Aggregate composer reach is often driven by",
            "a single breakout soundtrack rather than a broad catalog."]
        }
    )
)

# Add an annotation
anno_df = pd.DataFrame({
    "composer_primary_clean": ["Jeff Danna"],
    "y": [528_112],   # listener count you want to point at
    "text": [
        "A Montage of Heck: The Home Recordings (2015)\n528,112 listeners\nSingle album dominates composer's total"
    ]
})

annotation = (
    alt.Chart(anno_df)
    .mark_text(
        align="left",
        dx=20,
        dy=-40,
        fontSize=12,
        lineBreak="\n"
    )
    .encode(
        x=alt.X("composer_primary_clean:N", sort=top_composers),                  # keep labels ordered by total listeners
        y="y:Q",
        text="text:N"
    )
)

point = (
    alt.Chart(anno_df)
    .mark_point(size=120)
    .encode(
        x=alt.X("composer_primary_clean:N", sort=top_composers),                  # keep labels ordered by total listeners
        y="y:Q"
    )
)

chart + point + annotation

st.markdown("""
Findings: This chart shows that composer\\-level listener totals are highly concentrated, with several top composers’ aggregate reach driven largely by a single standout soundtrack\\. This suggests that composer prominence in soundtrack listening often reflects the success of one exceptional project rather than consistently high engagement across a broader body of work\\.
""")

st.markdown("""
TO DO: Color fix to album sequence
""")

st.markdown("""
### IV\\.2 Distribution of listener composer concentration histogram
""")

st.markdown("""
Question: To what extent is each composer’s overall listener reach concentrated in a single “standout” soundtrack album?
""")

st.markdown("""
This charttests whether a composer’s apparent popularity reflects a broad catalog or a single blockbuster soundtrack\\. For each composer, we aggregate listeners to the album level \\(summing listeners within composer–album to guard against duplicate rows\\), then compute the share of the composer’s total listeners attributable to their single most\\-listened album \\(top\\_album\\_share\\)\\. The histogram shows how these “top\\-album shares” are distributed across composers: values near 1\\.0 indicate a composer’s reach is almost entirely driven by one album, while lower values suggest a more diversified listener footprint spread across multiple soundtracks\\.
""")

# Aggregate to album-level within composer (safety in case of dup rows)
composer_album = (
    composer_df.groupby(["composer_primary_clean", "album_title"], as_index=False)
      .agg(album_listeners=("lfm_album_listeners", "sum"))
)

# Compute per-label totals and top-album contribution
composer_summary = (
    composer_album
    .groupby("composer_primary_clean")
    .agg(
        total_label_listeners=("album_listeners", "sum"),
        top_album_listeners=("album_listeners", "max"),
        album_count=("album_title", "nunique")
    )
    .reset_index()
)

composer_summary["top_album_share"] = (
    composer_summary["top_album_listeners"] /
    composer_summary["total_label_listeners"]
)

alt.data_transformers.disable_max_rows()

hist = (
    alt.Chart(composer_summary)
    .mark_bar()
    .encode(
        x=alt.X(
            "top_album_share:Q",
            bin=alt.Bin(step=0.05),
            title="Share of composer listeners from top album"
        ),
        y=alt.Y(
            "count():Q",
            title="Number of composers"
        ),
        tooltip=[
            alt.Tooltip("count():Q", title="Composers")
        ]
    )
    .properties(
        width=700,
        height=400,
        title={
            "text":"Distribution of Listener Concentration Across Composers",
            "subtitle": "Aggregate composer reach is dominated by one standout album in most cases"

        }
    )
)

# auto-detected possible Altair chart: hist
try:
    st.altair_chart(hist, use_container_width=True)
except Exception:
    st.write(hist)

st.markdown("""
Findings: This distribution shows that soundtrack listening is already strongly concentrated at the composer level\\. For most composers, a single album accounts for the majority—and often nearly all—of their total listeners, with top\\-album shares clustering close to 1\\.0\\. Only a small number of composers exhibit a more balanced distribution of listening across multiple soundtracks\\. This indicates that composer\\-level prominence in soundtrack listening is typically shaped by one breakout project rather than consistent engagement across a broader body of work, highlighting how much overall visibility hinges on exceptional individual releases\\.
""")

st.markdown("""
# V\\. Label Association and Album Popularity
""")

st.markdown("""
We explore how soundtrack album listening is distributed across record labels, focusing on whether label\\-level popularity reflects broad catalog performance or is driven by a small number of breakout titles\\.
""")

st.markdown("""
Question: How is soundtrack album listening distributed across record labels, and to what extent is label\\-level popularity driven by single breakout titles versus broader catalog performance?
Visualization: \\(1\\) Stacked bar chart of total album listeners for the top 15 record labels, with each bar decomposed by individual soundtrack albums\\. \\(2\\) Histogram of the share of each label’s total listeners contributed by its most\\-listened\\-to album\\.
""")

# Let's analyze the structure of label names
print(albums_df['label_names'].nunique())
print(albums_df['label_names'].value_counts().index.tolist())
print(albums_df['label_names'].value_counts().tolist())


st.markdown("""
It's apparent that label name are mostly single labels, but there are a handful of rare cases where multiple labels are involved\\. For the purpose of visualization, we'll use the raw values\\. We don't anticipate using label\\_names for correlation/regression analysis, so there is no need to translate them\\.
""")

st.markdown("""
### V\\.1 Stacked bar chart of top labels
""")

st.markdown("""
Question: Among the most\\-listened soundtrack labels, are total listeners driven by a broad catalog of albums—or by one or two breakout soundtracks?
""")

st.markdown("""
This chart examines listener concentration at the label level by focusing on the top 15 labels ranked by total Last\\.fm album listeners \\(after removing albums with missing/“\\[no label\\]” metadata\\)\\. Within each of these labels, albums are ranked by listener count, and the label’s total is visualized as a stacked bar where each stack segment represents an album bucketed by its rank within that label \\(e\\.g\\., \\#1, \\#2, …, \\#10, and \\#11\\+\\)\\. If a label’s bar is dominated by its “\\#1” \\(or a small handful of top\\-ranked segments\\), that signals the label’s reach is largely propelled by a single blockbuster soundtrack rather than consistently strong performance across many releases\\.
""")

labeled_albums_df = albums_df[(albums_df['label_names'] != '[no label]') & (albums_df['label_names'].notna())]

# print(labeled_albums_df.shape)   # 1466 albums (from 1551 originally, so 84 albums dropped)

# -----------------------------
# 0) Basic cleaning / guardrails
# -----------------------------

# Ensure year exists for tooltip (use US release year if available, else film year)
labeled_albums_df["album_year_for_tt"] = labeled_albums_df["album_us_release_year"]
labeled_albums_df.loc[labeled_albums_df["album_year_for_tt"].isna(), "album_year_for_tt"] = labeled_albums_df["film_year"]

# -----------------------------
# 1) Pick the "top 15 labels"
#    (by total listeners across albums)
# -----------------------------
top_labels = (
    labeled_albums_df.groupby("label_names", dropna=False)["lfm_album_listeners"]
      .sum()
      .sort_values(ascending=False)
      .head(15)
      .index
      .tolist()
)

df_top = labeled_albums_df[labeled_albums_df["label_names"].isin(top_labels)].copy()

# ------------------------------------------------------------
# 1b) Rank albums within each label by album listeners (desc)
#     rank=1 means "top album for this label"
# ------------------------------------------------------------
df_top["album_rank_in_label"] = (
    df_top.groupby("label_names")["lfm_album_listeners"]
    .rank(method="first", ascending=False)
    .astype(int)
)

TOP_K = 10  # or 8, 12, etc.

df_top["rank_bucket"] = df_top["album_rank_in_label"].where(
    df_top["album_rank_in_label"] <= TOP_K,
    TOP_K + 1
)

df_top["rank_bucket_label"] = df_top["rank_bucket"].map(
    lambda r: f"#{r}" if r <= TOP_K else f"#{TOP_K+1}+"
)

# -----------------------------
# 2) Build stacked bar chart
#    - x: label
#    - y: listeners
#    - stacks: album_title, with largest albums at the base (order=desc)
#    - tooltip: album title, year, raw listeners
# -----------------------------
alt.data_transformers.disable_max_rows()

chart = (
    alt.Chart(df_top)
    .mark_bar()
    .encode(
        x=alt.X(
            "label_names:N",
            sort=top_labels,                  # keep labels ordered by total listeners
            title="Label",
            axis = alt.Axis(labelAngle = -45)
        ),
        y=alt.Y(
            "sum(lfm_album_listeners):Q",
            title="Album listeners (sum)"
        ),
        color=alt.Color(    # Make sure all the albums of the same rank within a label have the same color
            "rank_bucket_label:N",
            title="Album rank within label",
            sort=[f"#{i}" for i in range(1, TOP_K+1)] + [f"#{TOP_K+1}+"],
            legend=alt.Legend(orient="right")
        ),
        # This controls the stacking order: biggest albums at the base
        order=alt.Order("lfm_album_listeners:Q", sort="descending"),
        tooltip=[
            alt.Tooltip("label_names:N", title="Label"),
            alt.Tooltip("album_title:N", title="Album"),
            alt.Tooltip("rank_bucket_label:N", title="Rank"),
            alt.Tooltip("album_year_for_tt:Q", title="Year", format=".0f"),
            alt.Tooltip("lfm_album_listeners:Q", title="Listeners", format=",.0f"),
        ],
    )
    .properties(
        width=750,
        height=400,
        title={
            'text': "Top 15 Labels — Total Album Listeners (stacked by album)",
            'subtitle': ["Label totals are often driven by a single breakout soundtrack",
            "rather than broad catalog performance."]
        }
    )
)

# Add an annotation
anno_df = pd.DataFrame({
    "label_names": ["Interscope Records"],
    "y": [1_034_714],   # listener count you want to point at
    "text": [
        "A Star Is Born (2018)\n1,034,714 listeners\nSingle album dominates label total"
    ]
})

annotation = (
    alt.Chart(anno_df)
    .mark_text(
        align="left",
        dx=40,
        dy=-20,
        fontSize=12,
        lineBreak="\n"
    )
    .encode(
        x=alt.X("label_names:N", sort=top_labels),                  # keep labels ordered by total listeners
        y="y:Q",
        text="text:N"
    )
)

point = (
    alt.Chart(anno_df)
    .mark_point(size=120)
    .encode(
        x=alt.X("label_names:N", sort=top_labels),                  # keep labels ordered by total listeners
        y="y:Q"
    )
)

chart + point + annotation

st.markdown("""
Findings: This visualization shows that total album listeners at the label level are frequently highly concentrated, with some labels’ aggregate reach driven almost entirely by a single breakout soundtrack rather than a broad catalog of moderately popular releases\\. For example, Interscope Records’ total listener count is overwhelmingly dominated by A Star Is Born \\(Soundtrack\\), which contributes the vast majority of listens for the label in this dataset\\. In contrast, other labels exhibit a more distributed pattern, where total listeners are accumulated across multiple albums with smaller individual footprints\\. This highlights an important distinction between catalog breadth and hit\\-driven impact: large aggregate listener counts do not necessarily imply consistent performance across a label’s soundtrack releases, but may instead reflect the presence of one exceptional title\\.
""")

st.markdown("""
### V\\.2 Distribution of listener label concentration histogram
""")

st.markdown("""
Question: How concentrated is each label’s total soundtrack listenership in its single most\\-popular album?
""")

st.markdown("""
This figure tests whether label\\-level “success” reflects a deep soundtrack catalog or a single blockbuster release\\. For each label, we first aggregate listeners to the label–album level \\(summing within each label and album to protect against duplicate rows\\), then compute top\\_album\\_share: the fraction of the label’s total listeners that come from its top\\-listened album\\. The histogram shows how those shares are distributed across labels—values near 1\\.0 mean a label’s listener reach is almost entirely explained by one standout soundtrack, while lower values indicate a more diversified listener base spread across multiple albums\\.
""")

# Aggregate to album-level within label (safety in case of dup rows)
label_album = (
    labeled_albums_df.groupby(["label_names", "album_title"], as_index=False)
      .agg(album_listeners=("lfm_album_listeners", "sum"))
)

# Compute per-label totals and top-album contribution
label_summary = (
    label_album
    .groupby("label_names")
    .agg(
        total_label_listeners=("album_listeners", "sum"),
        top_album_listeners=("album_listeners", "max"),
        album_count=("album_title", "nunique")
    )
    .reset_index()
)

label_summary["top_album_share"] = (
    label_summary["top_album_listeners"] /
    label_summary["total_label_listeners"]
)

alt.data_transformers.disable_max_rows()

hist = (
    alt.Chart(label_summary)
    .mark_bar()
    .encode(
        x=alt.X(
            "top_album_share:Q",
            bin=alt.Bin(step=0.05),
            title="Share of label listeners from top album"
        ),
        y=alt.Y(
            "count():Q",
            title="Number of labels"
        ),
        tooltip=[
            alt.Tooltip("count():Q", title="Labels")
        ]
    )
    .properties(
        width=700,
        height=400,
        title={
            "text":"Distribution of Listener Concentration Across Labels",
            "subtitle": ["Most labels’ soundtrack listener totals are overwhelmingly",
            "driven by a single breakout album."]
        }
    )
)

# auto-detected possible Altair chart: hist
try:
    st.altair_chart(hist, use_container_width=True)
except Exception:
    st.write(hist)

st.markdown("""
Findings: This distribution makes the pattern pretty clear: soundtrack listening is highly hit\\-driven, even at the label level\\. For most labels, a single album accounts for the vast majority of total listeners, with many clustering near a top\\-album share of 1\\.0\\. In other words, a label’s apparent scale is often explained by one breakout soundtrack rather than consistent performance across a broader catalog\\. Only a small number of labels show anything resembling a balanced distribution of listening across multiple releases, reinforcing that soundtrack popularity tends to hinge on exceptional titles tied to standout films\\.
""")

st.markdown("""
# VI\\. Within\\-album Popularity Concentration
""")

st.markdown("""
For each soundtrack, we measure within\\-album concentration by calculating what percentage of total listening comes from the top 1 track and the top 3 tracks\\.
""")

st.markdown("""
Question: Within a soundtrack album, is listening spread across tracks, or dominated by a few standouts?
Visualization: Histogram \\(or density plot\\) of Top\\-1 listening share and/or Top\\-3 listening share across albums\\.
""")

album_groups = wide_df.groupby("release_group_id")

rows = []
for album_id, g in album_groups:
    listeners = g["lfm_track_listeners"]   # should be a Series
    total = listeners.sum()

    # sort listeners descending
    top_sorted = listeners.sort_values(ascending=False)

    rows.append({
        "release_group_id": album_id,
        "album_title": g["album_title"].iloc[0],
        "top1_share": top_sorted.iloc[0] / total,
        "top3_share": top_sorted.iloc[:3].sum() / total
    })

source1 = pd.DataFrame(rows)

c1 = alt.Chart(source1).mark_bar().encode(
    x=alt.X("top1_share:Q", bin=alt.Bin(maxbins=10)),
    y=alt.Y("count():Q", title = "Album count")
).properties(
    title={
        "text": "Top-1 listening share across soundtrack albums",
        "subtitle": [
            "Most soundtracks avoid single-track dominance,",
            "but a non-trivial subset is dominated by a single hit."
        ],
        "subtitleFontSize": 11
    }
)

c2 = alt.Chart(source1).mark_bar().encode(
    x=alt.X("top3_share:Q", bin=alt.Bin(maxbins=10)),
    y=alt.Y("count():Q", title = None)
).properties(
    title={
        "text": "Top-3 listening share across soundtrack albums",
        "subtitle": [
            "Even when listening isn’t concentrated in one track",
            "it is often concentrated in just two or three."
        ],
        "subtitleFontSize": 11
    }
)

(c1 | c2).resolve_scale(y = "shared")

st.markdown("""
Findings: In Notebook 4\\.6 Part IV\\.1, we showed that album\\-level and track\\-level Last\\.fm metrics are moderately aligned in rank, indicating that more popular albums generally also have higher track\\-level engagement\\. This section looks one level deeper, asking how that track\\-level engagement is distributed within albums\\.
""")

st.markdown("""
The Top\\-1 and Top\\-3 share analysis shows that soundtrack listening is typically selective rather than evenly spread across all tracks\\. While most albums avoid complete single\\-track dominance, listening attention often concentrates in a small subset of tracks\\. In many cases, just two or three tracks account for a large share of total track\\-level listeners\\.
""")

st.markdown("""
These results help explain why album\\-level and track\\-level metrics align in rank without being tightly coupled numerically\\. Track\\-level engagement reflects which specific songs listeners gravitate toward, while album\\-level metrics summarize broader interest in the soundtrack as a whole\\. As a result, two albums with similar album\\-level popularity can exhibit very different internal listening patterns\\.
""")

st.markdown("""
Taken together with the earlier rank\\-coherence results, this analysis shows that album and track metrics are consistent but not redundant: album popularity sets the overall scale of attention, while track\\-level metrics reveal how that attention is distributed within the album\\.
""")

st.markdown("""
Implications / Directions for Further Exploration:
\\- Use listening concentration \\(e\\.g\\., Top\\-1 / Top\\-3 share\\) as a simple way to distinguish between “hit\\-driven” soundtracks and more ensemble\\-style albums\\.
\\- Look at whether hit\\-driven soundtracks tend to be tied to bigger or more commercially visible films, versus ensemble soundtracks that may behave differently\\.
\\- Check if certain kinds of soundtracks \\(e\\.g\\., orchestral scores vs pop\\-heavy albums\\) consistently show different listening patterns\\.
\\- Explore whether the relationship between film popularity and soundtrack popularity looks different depending on how listening is distributed within the album\\.
\\- Treat listening structure as a potential input for clustering soundtracks into a small number of recognizable consumption patterns\\.
""")

st.markdown("""
While these ideas point to interesting directions for deeper analysis, they are beyond the scope of this milestone\\. The goal here is simply to establish that soundtrack albums differ meaningfully in how listening is distributed across tracks\\. These differences do not directly affect the correlation and regression analyses that follow, but they provide useful context and suggest natural extensions that could be explored in a subsequent milestone\\.
""")

st.markdown("""
# VII\\. Album popularity vs track\\-level aggregations
""")

st.markdown("""
We'll do another analysis of popularity that goes a bit deeper compared to what we did earlier\\. What album\\-level popularity is actually reflecting by comparing it to different summaries of track popularity \\(best track vs typical track\\), so we can tell whether albums “win” because of one breakout or because multiple tracks carry weight\\.
""")

st.markdown("""
Question: Which track\\-level summary lines up best with album\\-level popularity: max, mean, or median track popularity?
Visualization: Three scatterplots: log\\(album popularity\\) vs log\\(max track popularity\\); log\\(album popularity\\) vs log\\(mean track popularity\\); log\\(album popularity\\) vs log\\(median track popularity\\)\\.
""")

# ------------------------------------------------------------
# Album vs Track Popularity (raw counts on log axes)
#
# Goal:
#   Compare album-level popularity to track-level popularity using three
#   track summaries per album:
#     1) Top track listeners (max)
#     2) Average track listeners (mean)
#     3) Typical track listeners (median)
#
# Why we use raw counts + log axes (instead of pre-logged columns):
#   - Log axes are easier to read when they show familiar powers of 10
#     (1, 10, 100, 1000, ...) rather than log-transformed values.
#   - We keep the natural interpretation of the metrics (counts), while
#     still compressing heavy tails.
#
# IMPORTANT:
#   - Log scales cannot display 0, so we filter albums where:
#       album_listeners <= 0 or top track listeners <= 0
#   - This visualization is descriptive (coherence / scale / structure),
#     not a test of additivity or equivalence between album and track metrics.
# ------------------------------------------------------------

# 1) Collapse wide_df to one row per album (release_group_id)
trackstats_by_album = (
    wide_df
    .groupby("release_group_id")
    .agg(
        album_title=("album_title", "first"),
        film_year=("film_year", "first"),

        # Track-level popularity summaries (raw counts)
        max_track_listeners=("lfm_track_listeners", "max"),
        mean_track_listeners=("lfm_track_listeners", "mean"),
        median_track_listeners=("lfm_track_listeners", "median"),

        # Album-level popularity (raw count; should be constant within album)
        album_listeners=("lfm_album_listeners", "max"),
    )
    .reset_index()
)

# 2) Filter out zeros: log axes cannot show 0 (and 0s create "stripes")
# Recall that we are using the wide_df table, where we purged track_playcounts of 0 but
# kept album_listeners == 0 or NaN because of the track-level granularity of wide_df
trackstats_by_album = trackstats_by_album[
    (trackstats_by_album["album_listeners"] > 0) &
    (trackstats_by_album["max_track_listeners"] > 0)
].copy()

# 3) Shared axis configuration: show powers of 10 (1, 10, 100, 1000, ...)
#    Extend the list if your data exceed these ranges.
pow10_ticks = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000]

x_shared = alt.X(
    "album_listeners:Q",
    title="Album listeners",
    scale=alt.Scale(type="log"),
    axis=alt.Axis(values=pow10_ticks, format="~s")  # ~s => 1k, 10k, 1M
)

# y-axis configs:
# - First chart shows y labels
# - Second/third hide y labels to reduce repetition
y_max_shared = alt.Y(
    "max_track_listeners:Q",
    title="Top track listeners",
    scale=alt.Scale(type="log"),
    axis=alt.Axis(values=pow10_ticks, format="~s")
)

# Helper for hidden y-axis labels (still shares the log scale + ticks internally)
def y_hidden(field: str):
    return alt.Y(
        f"{field}:Q",
        title=None,
        scale=alt.Scale(type="log"),
        axis=alt.Axis(labels=False, ticks=False, values=pow10_ticks)
    )

# 4) Build the three scatterplots (Album on x-axis, Track on y-axis)
base = alt.Chart(trackstats_by_album).mark_circle(opacity=0.35, size=40).encode(
    x=x_shared,
    tooltip=[
        alt.Tooltip("album_title:N", title="Album"),
        alt.Tooltip("film_year:Q", title="Release year", format="d"),
        alt.Tooltip("album_listeners:Q", title="Album listeners", format=","),
    ]
)

c1 = base.encode(
    y=y_max_shared,
    tooltip=[
        alt.Tooltip("album_title:N", title="Album"),
        alt.Tooltip("film_year:Q", title="Release year", format="d"),
        alt.Tooltip("album_listeners:Q", title="Album listeners", format=","),
        alt.Tooltip("max_track_listeners:Q", title="Top track listeners", format=","),
    ]
).properties(
    title={
        "text": "Max track popularity",
        "subtitle": "Album listeners vs top track listeners"
    }
)

c2 = base.encode(
    y=y_hidden("mean_track_listeners"),
    tooltip=[
        alt.Tooltip("album_title:N", title="Album"),
        alt.Tooltip("film_year:Q", title="Release year", format="d"),
        alt.Tooltip("album_listeners:Q", title="Album listeners", format=","),
        alt.Tooltip("mean_track_listeners:Q", title="Mean track listeners", format=",.0f"),
    ]
).properties(
    title={
        "text": "Mean track popularity",
        "subtitle": "Album listeners vs average track listeners"
    }
)

c3 = base.encode(
    y=y_hidden("median_track_listeners"),
    tooltip=[
        alt.Tooltip("album_title:N", title="Album"),
        alt.Tooltip("film_year:Q", title="Release year", format="d"),
        alt.Tooltip("album_listeners:Q", title="Album listeners", format=","),
        alt.Tooltip("median_track_listeners:Q", title="Median track listeners", format=",.0f"),
    ]
).properties(
    title={
        "text": "Median track popularity",
        "subtitle": "Album listeners vs typical track listeners"
    }
)

# 5) Print simple Pearson correlations on raw counts (descriptive only)
#    If you want rank-based association (less sensitive to heavy tails),
#    reuse your Spearman helper from earlier notebooks.
print("Correlation with album listeners (raw counts):\n")
print(f"Max track listeners:    {trackstats_by_album['album_listeners'].corr(trackstats_by_album['max_track_listeners']):.2f}")
print(f"Mean track listeners:   {trackstats_by_album['album_listeners'].corr(trackstats_by_album['mean_track_listeners']):.2f}")
print(f"Median track listeners: {trackstats_by_album['album_listeners'].corr(trackstats_by_album['median_track_listeners']):.2f}")

# 6) Render side-by-side charts
(c1 | c2 | c3)

st.markdown("""
Findings: Album popularity is only moderately associated with track\\-level popularity summaries, with no single summary—maximum, mean, or median—clearly dominating as an explanatory signal\\.
""")

st.markdown("""
Implications / Directions for Further Exploration:
\\- Album popularity should be modeled directly, rather than treated as a straightforward aggregation of track\\-level performance\\.
\\- Explanatory power is likely to come from album\\- and film\\-level context \\(e\\.g\\., film popularity, release timing\\), rather than from track\\-level summaries alone\\.
\\- Track\\-level metrics may be better suited for describing internal listening structure within albums, while album\\-level outcomes require separate analysis\\.
""")

st.markdown("""
TODO: Why is there a log\\(0\\) \\-\\- I thought we cleaned that out
""")

st.markdown("""
# VIII\\. Track position sensitivity
""")

st.markdown("""
Next, let's check whether track ordering alone correlates with listening, because if early tracks systematically outperform later ones, that’s a real structural bias in the data and it affects how we interpret within\\-album patterns\\.
""")

st.markdown("""
Question: Do earlier track positions tend to have higher popularity than later positions?
Visualization: Line plot of median log track popularity by track number\\.
""")

import pandas as pd
import altair as alt

alt.data_transformers.disable_max_rows()

# -----------------------------
# 1) Aggregate stats by track_number
# -----------------------------
stats = (
    wide_df
    .groupby("track_number")
    .agg(
        mean=("log_lfm_track_listeners", "mean"),
        std=("log_lfm_track_listeners", "std"),
        median=("log_lfm_track_listeners", "median"),
        q25=("log_lfm_track_listeners", lambda x: x.quantile(0.25)),
        q75=("log_lfm_track_listeners", lambda x: x.quantile(0.75)),
        n=("log_lfm_track_listeners", "count"),
    )
    .reset_index()
    .sort_values("track_number")
)

# Cap to <= 30 tracks (as you noted)
stats = stats[stats["track_number"] <= 30].copy()

# Mean ribbon bounds
stats["mean_lo"] = stats["mean"] - stats["std"]
stats["mean_hi"] = stats["mean"] + stats["std"]

# -----------------------------
# 2) Build LEFT: median + IQR ribbon
# -----------------------------
base = alt.Chart(stats).encode(
    x=alt.X("track_number:Q", title="Track number")
)

median_ribbon = base.mark_area(opacity=0.3).encode(
    y=alt.Y("q25:Q", title="log(track listeners)", axis = alt.Axis(orient = "left")),
    y2="q75:Q",
    tooltip=[
        alt.Tooltip("track_number:Q", title="Track #"),
        alt.Tooltip("median:Q", title="Median", format=".3f"),
        alt.Tooltip("q25:Q", title="P25", format=".3f"),
        alt.Tooltip("q75:Q", title="P75", format=".3f"),
        alt.Tooltip("n:Q", title="Tracks (n)", format=","),
    ],
)

median_line = base.mark_line(size=2).encode(
    y=alt.Y("median:Q", title="Log(track listeners)", axis=alt.Axis(orient="left"))
)

median_chart = (median_ribbon + median_line).properties(
    width=420,
    height=320,
    title={
        "text": "Median popularity by track position",
        "subtitle": ["Ribbon = IQR (P25–P75)"]
    }
)

# -----------------------------
# 3) Build RIGHT: mean + ±1 SD ribbon
# -----------------------------
mean_ribbon = base.mark_area(opacity=0.3).encode(
    y=alt.Y("mean_lo:Q", title=None, axis=alt.Axis(orient="left")),
    y2="mean_hi:Q",
    tooltip=[
        alt.Tooltip("track_number:Q", title="Track #"),
        alt.Tooltip("mean:Q", title="Mean", format=".3f"),
        alt.Tooltip("std:Q", title="SD", format=".3f"),
        alt.Tooltip("mean_lo:Q", title="Mean - SD", format=".3f"),
        alt.Tooltip("mean_hi:Q", title="Mean + SD", format=".3f"),
        alt.Tooltip("n:Q", title="Tracks (n)", format=","),
    ],
)

mean_line = base.mark_line(size=2).encode(
    y=alt.Y("mean:Q", title=None, axis=alt.Axis(orient="left"))
)

mean_chart = (mean_ribbon + mean_line).properties(
    width=420,
    height=320,
    title={
        "text": "Mean popularity by track position",
        "subtitle": ["Ribbon = ± 1 SD"]
    }
)

# -----------------------------
# 4) Side-by-side
# -----------------------------
(median_chart | mean_chart).resolve_scale(y="shared")

st.markdown("""
Findings: Track popularity declines modestly as track position increases, indicating a mild front\\-loading effect within soundtrack albums\\. However, the difference between the two panels is more revealing than the downward trend itself\\. The median ribbon remains relatively narrow across track positions, suggesting that typical listening behavior is fairly stable from track to track\\. By contrast, the much wider band around the mean reflects substantial variability driven by a small number of extremely popular tracks, which inflate average listening at early positions\\. Taken together, these patterns show that front\\-loading exists, but its apparent strength in average trends is amplified by a handful of breakout tracks rather than a broad shift in typical listener behavior\\.
""")

st.markdown("""
In other words, the median listener experience changes little across track positions, while the mean is heavily influenced by rare, high\\-impact tracks that tend to appear earlier in albums\\.
""")
