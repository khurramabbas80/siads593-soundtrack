import streamlit as st
import os, sys

st.set_page_config(page_title="5.4b Audio Features Visual Exploration", layout="wide")

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

import pandas as pd
import numpy as np
import os
from typing import List, Union
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

# Load the dataframe
tracks_clean_df = pd.read_csv("./pipeline/5.4.a.Tracks_clean.csv")

print(f"Total records in tracks_df: {tracks_clean_df.shape[0]}")
print(tracks_clean_df.columns)
tracks_clean_df.head()

# Calculate descriptive statistics for the cleaned dataframe

tracks_clean_df.describe()

st.markdown("""
# II\\. Exploration of Track Tempo, Happiness and Danceability
""")

# Helper function to create histograms with Altair

def create_histogram(
    df: pd.DataFrame,
    column: str,
    *,
    title: str,
    subtitle: str | None = None,
    bins: int = 30,
    width: int = 400,
    height: int = 300,
) -> alt.Chart:
    """
    Create a histogram using Altair.
    """

    # ------------------------------------------------------------
    # STEP 1: Input validation
    # ------------------------------------------------------------

    # Ensure the requested column exists in the dataframe.
    # Prevents silent failures or confusing Altair errors later.
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in dataframe.")

    # Histograms require numeric data.
    # If the column is not numeric, raise an error early.
    if not pd.api.types.is_numeric_dtype(df[column]):
        raise TypeError(f"Column '{column}' must be numeric to plot a histogram.")

    # ------------------------------------------------------------
    # STEP 2: Configure title and optional subtitle
    # ------------------------------------------------------------

    # Build a dictionary that Altair will use for the chart title.
    # We dynamically include a subtitle only if one is provided.
    title_config = {"text": title}

    if subtitle:
        title_config["subtitle"] = subtitle
        title_config["subtitleFontSize"] = 11  # Slightly smaller subtitle font

    # ------------------------------------------------------------
    # STEP 3: Build the histogram chart
    # ------------------------------------------------------------

    chart = (
        # Restrict the dataframe to only the relevant column.
        # This keeps the chart lightweight and avoids unintended fields.
        alt.Chart(df[[column]])

        # Use bar marks to represent bin counts.
        .mark_bar()

        # Define how data maps to axes.
        .encode(
            # X-axis:
            # - Treat column as quantitative (:Q)
            # - Apply binning (group continuous values into ranges)
            x=alt.X(
                f"{column}:Q",
                bin=alt.Bin(maxbins=bins),  # Number of bins controlled by `bins`
                title=column.replace("_", " ").title(),  # Clean axis label
            ),

            # Y-axis:
            # - count():Q automatically counts number of rows per bin
            y=alt.Y("count():Q", title="Count"),
        )

        # Apply chart-level properties
        .properties(
            title=title_config,  # Title + optional subtitle
            width=width,         # Chart width in pixels
            height=height,       # Chart height in pixels
        )
    )

    # ------------------------------------------------------------
    # STEP 4: Return the constructed Altair chart
    # ------------------------------------------------------------

    return chart


# Helper function to create box plots and violin plots with Altair

def box_violin_plot(
    df: pd.DataFrame,
    column: str,
    *,
    y_axis_title: str,
    box_title: str,
    violin_title: str,
    box_subtitle: str | None = None,
    violin_subtitle: str | None = None,
    box_width: int = 160,
    violin_width: int = 160,
):
    """
    Create a side-by-side boxplot and violin plot for a numeric column,
    with a soft orange median line extending across BOTH plots and an
    annotation on the violin plot.
    """

    # ------------------------------------------------------------
    # STEP 1: Input validation
    # ------------------------------------------------------------

    # Ensure a column name was provided
    if not column:
        raise ValueError("`column` must be provided.")

    # Ensure the column exists in the dataframe
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in dataframe.")

    # Box/violin plots require numeric data
    if not pd.api.types.is_numeric_dtype(df[column]):
        raise TypeError(f"Column '{column}' must be numeric to plot.")

    # Ensure required text parameters are not empty
    for name, value in {
        "y_axis_title": y_axis_title,
        "box_title": box_title,
        "violin_title": violin_title,
    }.items():
        if not value:
            raise ValueError(f"`{name}` must be provided and cannot be empty.")

    # ------------------------------------------------------------
    # STEP 2: Prepare source data
    # ------------------------------------------------------------

    # Restrict dataframe to the relevant column
    # Drop missing values so Altair statistics are correct
    source = df[[column]].dropna()

    # ------------------------------------------------------------
    # STEP 3: Configure titles
    # ------------------------------------------------------------

    # Build box plot title dictionary
    box_title_cfg = {"text": box_title}
    if box_subtitle:
        box_title_cfg["subtitle"] = box_subtitle
        box_title_cfg["subtitleFontSize"] = 11

    # Build violin plot title dictionary
    violin_title_cfg = {"text": violin_title}
    if violin_subtitle:
        violin_title_cfg["subtitle"] = violin_subtitle
        violin_title_cfg["subtitleFontSize"] = 11

    # ------------------------------------------------------------
    # STEP 4: Compute median value
    # ------------------------------------------------------------

    # Calculate median in pandas (not Altair) so:
    # - it is deterministic
    # - it can be reused in both charts
    # - it can be formatted cleanly
    median_val = float(source[column].median())

    # Create small dataframe for median overlay
    median_df = pd.DataFrame({
        column: [median_val],
        "label": [f"Median: {median_val:.2f}"]
    })

    # Use a colorblind-friendly orange for emphasis
    ORANGE = "#E69F00"

    # Horizontal rule at median value
    median_rule = (
        alt.Chart(median_df)
        .mark_rule(color=ORANGE, strokeWidth=2)
        .encode(y=alt.Y(f"{column}:Q"))
    )

    # ------------------------------------------------------------
    # STEP 5: Create Boxplot
    # ------------------------------------------------------------

    # Altair automatically computes quartiles + whiskers
    box = (
        alt.Chart(source)
        .mark_boxplot(size=50)
        .encode(
            y=alt.Y(f"{column}:Q", title=y_axis_title)
        )
        .properties(
            width=box_width,
            title=box_title_cfg,
        )
    )

    # Overlay median rule on boxplot
    box_with_median = box + median_rule

    # ------------------------------------------------------------
    # STEP 6: Create Violin plot
    # ------------------------------------------------------------

    # Violin is a mirrored density plot:
    # - transform_density estimates distribution
    # - stack="center" mirrors it horizontally
    violin_area = (
        alt.Chart(source)
        .transform_density(column, as_=[column, "density"])
        .mark_area(orient="horizontal")
        .encode(
            y=alt.Y(f"{column}:Q", title=None),
            x=alt.X(
                "density:Q",
                stack="center",
                title=None,
                axis=alt.Axis(labels=False, ticks=False, domain=False),
            ),
        )
    )

    # Add median label annotation to violin
    median_text = (
        alt.Chart(median_df)
        .mark_text(
            color=ORANGE,
            align="left",
            baseline="middle",
            dx=6,           # slight horizontal offset from rule
            fontSize=11,
        )
        .encode(
            y=alt.Y(f"{column}:Q"),
            x=alt.value(violin_width - 2),  # anchor near right edge
            text="label:N",
        )
    )

    # Combine violin layers
    violin_with_median = (
        (violin_area + median_rule + median_text)
        .properties(
            width=violin_width,
            title=violin_title_cfg,
        )
    )

    # ------------------------------------------------------------
    # STEP 7: Combine both charts side-by-side
    # ------------------------------------------------------------

    # | places charts horizontally
    # resolve_scale ensures both share identical y-axis scaling
    return (box_with_median | violin_with_median).resolve_scale(y="shared")

# Visualize the distribution of tempo

create_histogram(
    tracks_clean_df,
    column="tempo",
    title="Track tempo distribution",
    bins=20,
)

# Inspect the rows with low tempos (0-40 BPM)

mask_low_tempo = (
    tracks_clean_df["tempo"].between(0, 40, inclusive="both")
)

tracks_clean_df.loc[
    mask_low_tempo,
    ["track_title_cleaned", "composer_primary_clean", "film_title", "tempo"]
].sort_values("tempo", ascending=True)

st.markdown("""
We listened to a sample of the tracks with 0 tempo to ensure they were audible music\\. Tempo is measured in beats per minute \\(BPM\\)\\. In my experience as a DJ, it would be extremely unlikely for an industry standard algorithm to register music \\(even vocal acapellas with no beat\\) as 0 BPM\\. It is possible that the Soundnet or Spotify algorithm that produced these values was not as robust and encountered errors\\. We decided to remove these from the analysis\\.
""")

st.markdown("""
For songs on the high end around 200 BPM, we felt these were reasonable and didn't need to be eliminated as outliers\\. Doing some internet research, we found that it was not uncommon for songs to be in this range\\.
""")

# Filter the dataframe so that tracks with 0 tempo are excluded

tracks_clean_df = tracks_clean_df[tracks_clean_df['tempo'] > 0]
len(tracks_clean_df)

# Visualize tempo after cleaning

create_histogram(
    tracks_clean_df,
    column="tempo",
    title="Track tempo distribution",
    bins=20,
)

# Visualize as box and violin

box_violin_plot(
    tracks_clean_df,
    column="tempo",
    y_axis_title="Beats per minute",
    box_title="Track tempo",
    box_subtitle="Pace of the song",
    violin_title="Distribution of track tempo",
    violin_subtitle="Pace of the song",
    box_width=220,
    violin_width=220
)

st.markdown("""
We can see that the median tempo is 111 BPM\\. For reference with popular genres, this is close to the tempo of electronic house music \\(~120 BPM\\), which is interesting because many of the tracks in the movie soundtrack population are known to be classical/orchestral in nature and thus quite different from house\\. Upon further investigation, we found that 111 BPM is within the known range of tempos for classical music\\. This pace is called allegretto and is considered moderately fast \\(source: https://en\\.wikipedia\\.org/wiki/Tempo\\)\\.
""")

# Visualize the distribution of track happiness

create_histogram(
    tracks_clean_df,
    column="happiness",
    title="Track happiness distribution",
    bins=20,
)

# Create box and violin plots of happiness

box_violin_plot(
    tracks_clean_df,
    column="happiness",
    y_axis_title="Score",
    box_title="Track happiness",
    box_subtitle="Brightness / mood",
    violin_title="Distribution of track happiness",
    violin_subtitle="Brightness / mood",
    box_width=220,
    violin_width=220
)


st.markdown("""
We can see here that track happiness tends to be concentrated on the lower end, with a median of 18 out of a possible score of 100\\. While Soundnet does not explicitly reveal how it calculates the happiness score, as they brand themselves as a replacement to the Spotify API, it is likely they take a similar approach\\. The audio features on the Spotify API are now deprecated, but in the past they had a feature called "valence" that likely used a combination of tempo, energy, key, mode, and danceability to calculate how happy a song was \\(source: https://github\\.com/raffg/spotify\\_analysis?tab=readme\\-ov\\-file\\)\\.
""")

st.markdown("""
While we would expect there to be some sad songs in movie genres like dramas, we've all heard lighter, upbeat songs during movies as well \\(think John Williams \\- "Parade of the Ewoks"\\), so it is surprising to see such a high degree of low happiness scores\\. In conventional music theory, the key of a song can determine whether a song sounds happy or sad\\. In general, songs in major keys are considered happy sounding and songs in minor keys are sad sounding\\. Let's see how our population is comprised in terms of major and minor \\(this is the mode column in our data\\)\\.
""")

# Create bar chart for the breakdown of major/minor tracks (the mode column)

base = alt.Chart(tracks_clean_df[['mode']])

bars = base.mark_bar(size=60).encode(
    x=alt.X(
        "mode:N",
        title="Mode",
        axis=alt.Axis(labelAngle=-45, labelFontSize=14, titleFontSize=16)
    ),
    y=alt.Y(
        "count():Q",
        title="Count",
        axis=alt.Axis(labelFontSize=14, titleFontSize=16)
    )
)

labels = (
    base
    # 1) make counts a real field per mode
    .transform_aggregate(
        count="count()",
        groupby=["mode"]
    )
    # 2) compute total across groups
    .transform_joinaggregate(
        total="sum(count)"
    )
    # 3) compute pct
    .transform_calculate(
        pct="datum.count / datum.total"
    )
    .mark_text(dy=-8, fontSize=13)
    .encode(
        x=alt.X("mode:N"),
        y=alt.Y("count:Q"),
        text=alt.Text("pct:Q", format=".1%")
    )
)

(bars + labels).properties(
    width=350,
    height=350,
    title=alt.TitleParams(text="Distribution of Track Mode", fontSize=18)
)

st.markdown("""
We can see that the majority of our tracks are in major keys \\(63%\\), which conflicts with the high concentration of low happiness scores that we saw earlier\\. Going forward, we will rely on mode instead of happiness to examine the mood of the songs\\. This means that the majority of movie soundtrack songs are happy sounding, not sad\\.
""")

# Visualize the distribution of track danceability

create_histogram(
    tracks_clean_df,
    column="danceability",
    title="Track danceability distribution",
    bins=20,
)

# Create box and violin plots of danceability

box_violin_plot(
    tracks_clean_df,
    column="danceability",
    y_axis_title="Score",
    box_title="Track danceability",
    box_subtitle="Groove-ability of the track",
    violin_title="Distribution of track danceability",
    violin_subtitle="Groove-ability of the track",
    box_width=220,
    violin_width=220
)

st.markdown("""
For danceability, we see a median score of 44 out of 100, with a peak in the 20's\\. This seems plausible given that we don't think of orchestral/instrumental movie tracks as bringing people to the dancefloor like electronic, hip hop, or pop\\.  
""")

st.markdown("""
# III\\. Exploration of Top Composers \\(by play count\\)
""")

# Helper function to rank composers by playcount

def rank_composers_by_playcount(
    df: pd.DataFrame,
    composer_col: str = "composer_primary_clean",
    playcount_col: str = "lfm_track_playcount",
    top_n: int | None = None,
) -> pd.DataFrame:
    """
    Rank composers (descending) by total playcount.

    Returns a dataframe with:
      - composer_col
      - total_playcount
      - rank (1 = highest)
    """

    # ------------------------------------------------------------
    # STEP 1: Input validation
    # ------------------------------------------------------------

    # Ensure the composer column exists
    if composer_col not in df.columns:
        raise KeyError(f"Column '{composer_col}' not found in dataframe.")

    # Ensure the playcount column exists
    if playcount_col not in df.columns:
        raise KeyError(f"Column '{playcount_col}' not found in dataframe.")

    # Ensure playcount column is numeric (since we will sum it)
    if not pd.api.types.is_numeric_dtype(df[playcount_col]):
        raise TypeError(f"Column '{playcount_col}' must be numeric.")

    # ------------------------------------------------------------
    # STEP 2: Aggregate total playcount per composer
    # ------------------------------------------------------------

    ranked = (
        # Keep only the relevant columns
        df[[composer_col, playcount_col]]

        # Drop rows where composer or playcount is missing
        # (we cannot aggregate properly without both)
        .dropna(subset=[composer_col, playcount_col])

        # Group rows by composer
        # as_index=False keeps composer as a column instead of index
        .groupby(composer_col, as_index=False)[playcount_col]

        # Sum playcount across all tracks for each composer
        .sum()

        # Rename aggregated column to something explicit
        .rename(columns={playcount_col: "total_playcount"})

        # Sort composers from highest to lowest total playcount
        .sort_values("total_playcount", ascending=False)

        # Reset index for clean, sequential numbering
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------
    # STEP 3: Create ranking column
    # ------------------------------------------------------------

    # Rank composers based on total_playcount
    # method="dense":
    #   - No gaps in rank values
    #   - If two composers tie for rank 1, next rank is 2 (not 3)
    # ascending=False ensures highest playcount = rank 1
    ranked["rank"] = (
        ranked["total_playcount"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    # ------------------------------------------------------------
    # STEP 4: Optionally limit to top N composers
    # ------------------------------------------------------------

    if top_n is not None:
        ranked = ranked.head(top_n).reset_index(drop=True)

    # ------------------------------------------------------------
    # STEP 5: Return final ranking dataframe
    # ------------------------------------------------------------

    return ranked

# Helper function to create faceted violin plots

def faceted_violin_plot(
    df: pd.DataFrame,
    value_col: str,
    *,
    facet_col: str,
    color_col: str,
    title: str,
    subtitle: str | None = None,
    width: int = 140,
    height: int = 300,
) -> alt.Chart:
    """
    Faceted violin plot with facet labels displayed horizontally and centered
    below each violin.
    """

    # ------------------------------------------------------------
    # STEP 1: Input validation
    # ------------------------------------------------------------

    # Ensure required columns exist in dataframe.
    # Even though color_col is not used for coloring anymore,
    # we still validate it to maintain compatibility with prior usage.
    for c in [value_col, facet_col, color_col]:
        if c not in df.columns:
            raise KeyError(f"Column '{c}' not found in dataframe.")

    # Violin plots require numeric data because we estimate a density.
    if not pd.api.types.is_numeric_dtype(df[value_col]):
        raise TypeError(f"Column '{value_col}' must be numeric.")

    # ------------------------------------------------------------
    # STEP 2: Configure chart title and optional subtitle
    # ------------------------------------------------------------

    # Build title dictionary dynamically.
    # If subtitle is provided, include it with smaller font.
    title_cfg = {"text": title}
    if subtitle:
        title_cfg["subtitle"] = subtitle
        title_cfg["subtitleFontSize"] = 11

    # ------------------------------------------------------------
    # STEP 3: Create the violin layer
    # ------------------------------------------------------------

    violin = (
        alt.Chart(df)

        # transform_density:
        #   - Computes a kernel density estimate (KDE)
        #   - Produces a smooth distribution curve
        #   - groupby ensures a separate density is computed for each facet group
        .transform_density(
            value_col,
            as_=[value_col, "density"],
            groupby=[facet_col, color_col],
        )

        # mark_area creates the filled shape of the violin
        .mark_area(orient="horizontal")

        # Encoding:
        .encode(

            # X-axis:
            # - density values (how thick the violin is at each y-value)
            # - stack="center" mirrors the density left and right
            # - axis elements removed for cleaner appearance
            x=alt.X(
                "density:Q",
                stack="center",
                title=None,
                axis=alt.Axis(labels=False, ticks=False, domain=False),
            ),

            # Y-axis:
            # - original numeric variable
            # - this determines the vertical position within the violin
            y=alt.Y(
                f"{value_col}:Q",
                title=value_col.replace("_", " ").title(),
            ),

            # No color encoding → violins use notebook theme default color
        )

        # Set panel size for each violin
        .properties(width=width, height=height)
    )

    # ------------------------------------------------------------
    # STEP 4: Facet into multiple violins (one per category)
    # ------------------------------------------------------------

    chart = violin.facet(

        # Create one column per unique value in facet_col
        column=alt.Column(
            f"{facet_col}:N",

            # Customize how facet labels appear
            header=alt.Header(
                labelAngle=0,          # horizontal labels
                labelOrient="bottom",  # place labels below violins
                labelAlign="center",   # center alignment
                labelBaseline="top",
            ),

            title=None,  # Remove redundant facet title
        )

    ).properties(
        title=title_cfg  # Apply main chart title + subtitle
    )

    # ------------------------------------------------------------
    # STEP 5: Ensure shared y-axis scale
    # ------------------------------------------------------------

    # resolve_scale(y="shared") ensures:
    #   - All violins use the same y-axis scale
    #   - Makes comparisons across categories meaningful
    return chart.resolve_scale(y="shared")

# View the top 10 composers by track play count

ranked = rank_composers_by_playcount(tracks_clean_df, top_n=10)

# auto-detected possible Altair chart: ranked
try:
    st.altair_chart(ranked, use_container_width=True)
except Exception:
    st.write(ranked)

st.markdown("""
We noticed some records had an "Unknown" composer name\\. Below we inspect these rows and replace them with the values we have for recording artist\\.
""")

# Inspect rows with "unknown" composer

tracks_clean_df.loc[
    tracks_clean_df["composer_primary_clean"]
        .str.strip()
        .str.lower()
        .eq("unknown"),
    [
        "track_title_cleaned",
        "recording_artist_credit",
        "film_title",
        "spotify_track_id",
        "composer_primary_clean",
    ]
]

# Fill the unknown composers with the recording artist names

mask_unknown = (
    tracks_clean_df["composer_primary_clean"]
        .str.strip()
        .str.lower()
        .eq("unknown")
)

tracks_clean_df.loc[mask_unknown, "composer_primary_clean"] = (
    tracks_clean_df.loc[mask_unknown, "recording_artist_credit"]
)

# Prepare dataframe with only tracks for the top 7 composers (reduced from 10 for more effective visualizations)

ranked = rank_composers_by_playcount(tracks_clean_df, top_n=7)

top_composers = ranked["composer_primary_clean"]
df_top = tracks_clean_df[tracks_clean_df["composer_primary_clean"].isin(top_composers)].copy()

# Visualize tempo by composer

faceted_violin_plot(
    df_top,
    value_col="tempo",
    facet_col="composer_primary_clean",
    color_col="composer_primary_clean",
    title="Tempo distributions by composer",
    subtitle="Sorted by name",
    width=100,
    height=260
)

st.markdown("""
For the top 7 composers by play count, we can see that the tempo distributions are very similar, with the exception of Maxim Nucci who appears to have a peak around 120 BPM\\. Let's examine his data more closely\\.
""")

# Calculate Nucci's median tempo

maxim_median_tempo = (
    tracks_clean_df.loc[tracks_clean_df["composer_primary_clean"] == "Maxim Nucci", "tempo"]
    .median()
)

print(f"The median tempo for Maxim Nucci is {maxim_median_tempo}")

# Inspect the top played tracks associated with Maxim Nucci as the primary composer

maxim_tracks_sorted = (
    tracks_clean_df.loc[
        tracks_clean_df["composer_primary_clean"] == "Maxim Nucci",
        ["track_title_cleaned", "film_title", "lfm_track_playcount"]
    ]
    .dropna(subset=["track_title_cleaned", "lfm_track_playcount"])
    .sort_values("lfm_track_playcount", ascending=False)
    .reset_index(drop=True)
)

# auto-detected possible Altair chart: maxim_tracks_sorted
try:
    st.altair_chart(maxim_tracks_sorted, use_container_width=True)
except Exception:
    st.write(maxim_tracks_sorted)

st.markdown("""
We listened to a sample of songs from this film on Spotify, and it appears to be a compilation with several genres\\. Some tracks, like Crocodile Ranger, are rock\\-oriented and quite upbeat\\. This might explain the tempo peak for Maxim Nucci relative to composers like Alan Silvestri, who is known for more classical scores for films like The Avengers\\.
""")

# Visualize the breakdown between major and minor modes for the top 7 composers

base = alt.Chart(df_top[["composer_primary_clean", "mode"]].dropna())

bars = base.mark_bar().encode(
    x=alt.X(
        "composer_primary_clean:N",
        title="Composer",
        axis=alt.Axis(labelAngle=-45, labelFontSize=12, titleFontSize=14)
    ),
    y=alt.Y(
        "count():Q",
        stack="normalize",
        title="Percent of tracks",
        axis=alt.Axis(format="%", labelFontSize=12, titleFontSize=14)
    ),
    color=alt.Color("mode:N", title="Mode")
)

bars.properties(
    width=650,
    height=400,
    title=alt.TitleParams(text="Mode breakdown by composer (Top 7)", fontSize=18)
)

# Display the percentages for major/minor

composer_col = "composer_primary_clean"
playcount_col = "lfm_track_playcount"
mode_col = "mode"

# 1) Top 7 composers by TOTAL playcount (descending)
top7 = (
    tracks_clean_df
    .dropna(subset=[composer_col, playcount_col])
    .groupby(composer_col, as_index=False)[playcount_col]
    .sum()
    .rename(columns={playcount_col: "total_playcount"})
    .sort_values("total_playcount", ascending=False)
    .head(7)
)

top7_composers = top7[composer_col].tolist()

# 2) Filter to top 7 and compute counts per composer x mode
counts = (
    tracks_clean_df.loc[
        tracks_clean_df[composer_col].isin(top7_composers),
        [composer_col, mode_col]
    ]
    .dropna(subset=[composer_col, mode_col])
    .groupby([composer_col, mode_col])
    .size()
    .reset_index(name="n")
)

# 3) Convert counts to within-composer percentages
counts["pct"] = counts["n"] / counts.groupby(composer_col)["n"].transform("sum")

# 4) Pivot to wide: one row per composer, columns major/minor
mode_pct_top7 = (
    counts.pivot(index=composer_col, columns=mode_col, values="pct")
    .fillna(0)
    .reindex(top7_composers)  # keep the same order as playcount ranking
    .reset_index()
)

# 5) Ensure both columns exist even if missing in data
for col in ["major", "minor"]:
    if col not in mode_pct_top7.columns:
        mode_pct_top7[col] = 0.0

# 6) Keep only requested columns
mode_pct_top7 = mode_pct_top7[[composer_col, "major", "minor"]]

# Convert proportions to 2 decimal places
mode_pct_top7[["major", "minor"]] = (
    mode_pct_top7[["major", "minor"]]
    .round(2)
)

# auto-detected possible Altair chart: mode_pct_top7
try:
    st.altair_chart(mode_pct_top7, use_container_width=True)
except Exception:
    st.write(mode_pct_top7)

st.markdown("""
We can see from the bar chart and the table of percentages that all 7 of the composers have a majority of their tracks in major keys\\. This means that most tracks by the top 7 composers are considered happy sounding in terms of music theory\\. 
""")

st.markdown("""
# IV\\. Exploration of Happiness Differences by Film Genre
""")

st.markdown("""
Similar to what we did for the top 7 composers, we are interested in seeing how certain film genres vary in terms of happiness \\(as measured by mode\\)\\.
""")

# Helper function to convert the film genre columns into long form for Altair

def explode_genres_for_violin(
    df,
    genre_cols,
    value_col,
    id_cols=None,
    genre_col_name="genre"
):
    """
    Convert wide boolean genre columns into a long format suitable for
    faceted violin plots.

    Parameters
    ----------
    df : pd.DataFrame
        Source dataframe (one row per song).
    genre_cols : list[str]
        List of boolean genre indicator columns (True/False).
    value_col : str
        Column containing the numeric value to plot (e.g. tempo, energy).
    id_cols : list[str], optional
        Columns to carry through unchanged (e.g. song_id, movie_id).
    genre_col_name : str, default "genre"
        Name of the output genre column.

    Returns
    -------
    pd.DataFrame
        Long-format dataframe with one row per (song, genre).
    """

    if id_cols is None:
        id_cols = []

    df_long = (
        df
        .melt(
            id_vars=id_cols + [value_col],
            value_vars=genre_cols,
            var_name=genre_col_name,
            value_name="is_genre"
        )
        .query("is_genre")
        .drop(columns="is_genre")
    )

    # Clean genre names
    df_long[genre_col_name] = (
        df_long[genre_col_name]
        .str.replace("film_is_", "", regex=False)
        .str.replace("_bool", "", regex=False)
    )

    return df_long

# Specify the genres of interest

genre_cols = [
    "film_is_drama",
    "film_is_comedy",
    "film_is_action",
    "film_is_horror",
    "film_is_family",
    "film_is_romance"
]

# Convert the genres to long form for Altair

df_genre_mode = explode_genres_for_violin(
    df=tracks_clean_df,
    genre_cols=genre_cols,
    value_col="mode",                # carry mode through
    id_cols=["track_id", "tmdb_id"]   # keep ids
).rename(columns={"mode": "mode"})   # (no-op, just explicit)

df_genre_mode = df_genre_mode.drop_duplicates(subset=["track_id", "genre"])


# Create the bar chart

base = alt.Chart(df_genre_mode[["genre", "mode"]].dropna())

bars = base.mark_bar().encode(
    x=alt.X(
        "genre:N",
        title="Genre",
        axis=alt.Axis(labelAngle=-45, labelFontSize=12, titleFontSize=14)
    ),
    y=alt.Y(
        "count():Q",
        stack="normalize",
        title="Percent of tracks",
        axis=alt.Axis(format="%", labelFontSize=12, titleFontSize=14)
    ),
    color=alt.Color("mode:N", title="Mode")
)

bars.properties(
    width=650,
    height=400,
    title=alt.TitleParams(text="Mode breakdown by genre", fontSize=18)
)

# ------------------------------------------------------------
# Display the percentage breakdowns for each genre
# ------------------------------------------------------------
# Goal:
# Create a table where:
#   • Each row = one genre
#   • Columns = "major" and "minor"
#   • Values = proportion of tracks in that genre with each mode
#   • Rounded to 2 decimal places (still proportions, not 0–100)
# ------------------------------------------------------------


# ------------------------------------------------------------
# STEP 1: Count tracks per (genre, mode)
# ------------------------------------------------------------

genre_mode_pct = (
    df_genre_mode[["genre", "mode"]]   # Keep only relevant columns

    # Remove rows missing genre or mode
    .dropna(subset=["genre", "mode"])

    # Group by genre and mode combination
    .groupby(["genre", "mode"])

    # Count number of rows in each group
    .size()

    # Convert result to dataframe with explicit column name
    .reset_index(name="n")
)

# At this stage the dataframe looks like:
# genre     mode     n
# drama     major    42
# drama     minor    18
# comedy    major    30
# comedy    minor    25
# ...


# ------------------------------------------------------------
# STEP 2: Convert counts into within-genre proportions
# ------------------------------------------------------------

# For each genre, divide each mode count by the total count for that genre.
genre_mode_pct["pct"] = (
    genre_mode_pct["n"]
    / genre_mode_pct.groupby("genre")["n"].transform("sum")
)

# Now "pct" represents:
#   proportion of tracks in that genre with that mode


# ------------------------------------------------------------
# STEP 3: Pivot from long → wide format
# ------------------------------------------------------------

# Convert from:
#   one row per (genre, mode)
# to:
#   one row per genre
#   columns = major, minor
genre_mode_pct_wide = (
    genre_mode_pct
    .pivot(index="genre", columns="mode", values="pct")

    # Replace missing values with 0
    # (e.g., if a genre has only major and no minor tracks)
    .fillna(0)

    # Move genre from index back to column
    .reset_index()
)

# After pivot:
# genre     major     minor
# drama     0.70      0.30
# comedy    0.55      0.45
# ...


# ------------------------------------------------------------
# STEP 4: Ensure both columns exist
# ------------------------------------------------------------

# If for some reason "major" or "minor" does not exist
# (e.g., dataset only contains one mode),
# create the missing column and fill with 0.
for col in ["major", "minor"]:
    if col not in genre_mode_pct_wide.columns:
        genre_mode_pct_wide[col] = 0.0


# ------------------------------------------------------------
# STEP 5: Keep only requested columns and round
# ------------------------------------------------------------

# Enforce column order
genre_mode_pct_wide = genre_mode_pct_wide[["genre", "major", "minor"]]

# Round proportions to 2 decimal places
genre_mode_pct_wide[["major", "minor"]] = (
    genre_mode_pct_wide[["major", "minor"]]
    .round(2)
)

# Final result:
#   • One row per genre
#   • major/minor columns contain proportions (0–1)
#   • Rounded to two decimals

# auto-detected possible Altair chart: genre_mode_pct_wide
try:
    st.altair_chart(genre_mode_pct_wide, use_container_width=True)
except Exception:
    st.write(genre_mode_pct_wide)

st.markdown("""
We can see in the stacked bar chart and the accompanying table that the genres of interest all have a majority of happy sounding tracks \\(major mode\\)\\. Family films have the highest proportion of major mode tracks at 67%, while drama and action share the lowest proportion at 62%\\. Unsurprisingly, comedy has more happy sounding tracks than drama at 66%\\.
""")

st.markdown("""
# V\\. Write Tracks Dataframe to File
""")

len(tracks_clean_df)

# Save track file to CSV

out_path = "./pipeline/5.4.b.Tracks_tempo_composer_clean.csv"

tracks_clean_df.to_csv(out_path, index=False)
