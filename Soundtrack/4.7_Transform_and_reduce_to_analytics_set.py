import streamlit as st
import os, sys

st.set_page_config(page_title="4.7 Transform and reduce to analytics set", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# I\\. Setup and load dataframes
""")

import pandas as pd
import numpy as np
import os

pd.set_option("display.float_format", "{:.2f}".format)

# Load the dataframes
albums_df = pd.read_csv("./pipeline/4.5.Albums_join_everything.csv")
artists_df = pd.read_csv("./pipeline/4.5.Artists_join_everything.csv")
tracks_df = pd.read_csv("./pipeline/4.5.Tracks_join_everything.csv")
wide_df = pd.read_csv("./pipeline/4.5.Wide_join_everything.csv")

st.markdown("""
# I\\.2 Cross\\-entity helper functions
""")

def drop_missing_or_nonpositive(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Drop rows where `col` is NaN or <= 0.
    Used for any Last.fm count metric we want to gate on.
    """
    return df.loc[df[col].notna() & (df[col] > 0)].copy()


def safe_log(series: pd.Series) -> pd.Series:
    """
    Log-transform safely:
      - log(x) when x > 0
      - NaN otherwise
    """
    return np.where(series > 0, np.log(series), np.nan)


def add_log_cols(df: pd.DataFrame, cols: list[str], prefix: str = "log_") -> pd.DataFrame:
    """
    Add log-transformed columns for each metric in `cols`.
    Creates new columns named f"{prefix}{col}".
    """
    out = df.copy()
    for c in cols:
        out[f"{prefix}{c}"] = safe_log(out[c])
    return out

st.markdown("""
# II\\. Transform and reduce albums
""")

st.markdown("""
Let's tackle albums first\\.\\.\\.
""")

# =============================================================================
# II. Transform and Reduce to Analytics Set (ALBUMS)
# -----------------------------------------------------------------------------
# Goal:
#   Create albums_analytics_df: a clean “analytics-ready” subset of albums_df
#   that:
#     1) Keeps only canonical soundtrack albums for films with sufficient vote_count
#     2) Removes rows where Last.fm album listeners are missing or zero
#     3) Adds log-transformed Last.fm album metrics for analysis
#
# Why we do this:
#   - Canonical soundtrack filter ensures a stable 1:1-ish film→album representation
#     (based on our canonical rules), reducing duplication and ambiguity.
#   - vote_count_above_500 is a quality gate so we focus on films with enough
#     TMDB activity to be meaningful in downstream analyses.
#   - Dropping 0/NaN listeners removes “no signal” rows that can distort
#     distributional analysis and break log transforms.
#   - Log transforms help stabilize heavy-tailed popularity distributions
#     (common with playcounts/listeners).
# =============================================================================


# -----------------------------------------------------------------------------
# Album-specific helper functions
# -----------------------------------------------------------------------------
def filter_to_canonical_high_confidence_albums(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter to the albums we consider "analytics eligible" for this milestone:
      - is_canonical_soundtrack == True
      - vote_count_above_500 == True

    Assumptions:
      - The incoming dataframe already has these boolean columns.
      - We do NOT deduplicate or rename columns here (keeps the notebook readable).
    """
    mask = (df["is_canonical_soundtrack"] == True) & (df["vote_count_above_500"] == True)
    return df.loc[mask].copy()

st.markdown("""
QA block\\. This helper prints a quick, standardized QA snapshot for the album\\-level transformation pipeline\\. It makes the filtering steps auditable by showing row\\-count attrition \\(baseline → canonical/vote filter → listener\\-clean\\), quantifying how many missing/zero listener values were removed, and sanity\\-checking the remaining listener distribution \\(min/median/max\\)
""")

def qa_print_album_transform_stats(
    df_before: pd.DataFrame,
    df_after_canonical: pd.DataFrame,
    df_after_listener_clean: pd.DataFrame,
    listeners_col: str = "lfm_album_listeners"
) -> None:
    """
    Print lightweight QA stats so teammates can sanity-check the transformation.
    Keeps the notebook transparent and easy to audit.
    """
    print("ALBUM ANALYTICS TRANSFORM — QA SUMMARY")
    print("-" * 60)

    # Row counts
    print(f"Rows before:                          {len(df_before):,}")
    print(f"After canonical + vote_count filter:  {len(df_after_canonical):,}")
    print(f"After drop missing/0 listeners:       {len(df_after_listener_clean):,}")

    # Listener completeness + range
    missing_listeners = df_after_canonical[listeners_col].isna().sum()
    zero_listeners = (df_after_canonical[listeners_col] == 0).sum()

    print("-" * 60)
    print(f"Listener NaNs removed (from filtered): {missing_listeners:,}")
    print(f"Listener zeros removed (from filtered): {zero_listeners:,}")

    if len(df_after_listener_clean) > 0:
        print("-" * 60)
        print("Post-clean listener sanity checks:")
        print(f"  min listeners: {df_after_listener_clean[listeners_col].min():,}")
        print(f"  median:        {df_after_listener_clean[listeners_col].median():,}")
        print(f"  max listeners: {df_after_listener_clean[listeners_col].max():,}")
    print("-" * 60)
    print()

# -----------------------------------------------------------------------------
# Build albums_analytics_df
# -----------------------------------------------------------------------------
# Step 0: keep the raw df as the “before” reference
albums_before_df = albums_df.copy()

# Step 1: Filter to canonical soundtrack albums where TMDB vote_count is strong enough
albums_filtered_df = filter_to_canonical_high_confidence_albums(albums_before_df)

# Step 2: Drop rows with no usable Last.fm listener signal (0 or NaN)
albums_clean_df = drop_missing_or_nonpositive(albums_filtered_df, "lfm_album_listeners")

# Step 3: Add log transforms for album listeners + playcount
albums_analytics_df = add_log_cols(
    albums_clean_df,
    cols=["lfm_album_listeners", "lfm_album_playcount"]
)

# QA printout (optional but recommended in the notebook)
qa_print_album_transform_stats(
    df_before=albums_before_df,
    df_after_canonical=albums_filtered_df,
    df_after_listener_clean=albums_analytics_df,
    listeners_col="lfm_album_listeners"
)

# At this point, albums_analytics_df is ready for downstream analysis.
# Let's take a quick peek:
st.dataframe(albums_analytics_df.head(3).round(2))
st.dataframe(albums_analytics_df[["lfm_album_listeners","log_lfm_album_listeners","lfm_album_playcount","log_lfm_album_playcount"]].describe().round(2))

st.markdown("""
Findings: After applying canonical soundtrack and vote\\-count quality gates, then removing albums with no listener signal, the final albums analytics set contains 1,551 soundtrack albums with valid Last\\.fm engagement\\. As expected, both listener and playcount distributions are highly right\\-skewed, with a small number of breakout soundtracks driving the upper tail\\. The log transforms substantially compress this skew, yielding more stable, analysis\\-ready variables that better support correlation analysis, modeling, and cross\\-film comparisons in downstream sections\\.
""")

st.markdown("""
# III\\. Transform and reduce artists
""")

# =============================================================================
# III. Transform and Reduce to Analytics Set (ARTISTS)
# -----------------------------------------------------------------------------
# Goal:
#   Create artists_analytics_df: an analytics-ready subset of artists_df that:
#     1) Removes artists with no usable Last.fm listener signal (NaN or 0)
#     2) Adds log-transformed Last.fm artist popularity metrics
#
# Why we do this (vs the album approach):
#   - Artists are already a derived "spine universe" (artists appearing on
#     soundtrack release groups). Applying film-level gates (canonical soundtrack,
#     vote_count thresholds) would arbitrarily exclude artists and conflate
#     artist popularity with film popularity.
#   - We gate on listeners because it is the cleanest indicator that Last.fm
#     returned meaningful engagement data for that artist.
# =============================================================================

def qa_print_artist_transform_stats(
    df_before: pd.DataFrame,
    df_after_listener_clean: pd.DataFrame,
    listeners_col: str = "lfm_artist_listeners"
) -> None:
    """
    Print lightweight QA stats so teammates can sanity-check the transformation.
    """
    print("ARTIST ANALYTICS TRANSFORM — QA SUMMARY")
    print("-" * 60)

    print(f"Rows before:                     {len(df_before):,}")
    print(f"After drop missing/0 listeners:  {len(df_after_listener_clean):,}")

    missing_listeners = df_before[listeners_col].isna().sum()
    zero_listeners = (df_before[listeners_col] == 0).sum()

    print("-" * 60)
    print(f"Listener NaNs removed: {missing_listeners:,}")
    print(f"Listener zeros removed: {zero_listeners:,}")

    if len(df_after_listener_clean) > 0:
        print("-" * 60)
        print("Post-clean listener sanity checks:")
        print(f"  min listeners: {df_after_listener_clean[listeners_col].min():,}")
        print(f"  median:        {df_after_listener_clean[listeners_col].median():,}")
        print(f"  max listeners: {df_after_listener_clean[listeners_col].max():,}")
    print("-" * 60)
    print()

# -----------------------------------------------------------------------------
# Build artists_analytics_df
# -----------------------------------------------------------------------------
# Step 0: keep the raw df as the “before” reference
artists_before_df = artists_df.copy()

# Step 1: Drop rows with no usable Last.fm listener signal (0 or NaN)
artists_clean_df = drop_missing_or_nonpositive(artists_before_df, "lfm_artist_listeners")

# Step 2: Add log transforms for artist listeners + playcount
artists_analytics_df = add_log_cols(
    artists_clean_df,
    cols=["lfm_artist_listeners", "lfm_artist_playcount"]
)

# QA printout (optional: recommended only in the notebook)
qa_print_artist_transform_stats(
    df_before=artists_before_df,
    df_after_listener_clean=artists_analytics_df,
    listeners_col="lfm_artist_listeners"
)


st.dataframe(artists_analytics_df.head(3).round(2))
display(artists_analytics_df[["lfm_artist_listeners","log_lfm_artist_listeners",
                             "lfm_artist_playcount","log_lfm_artist_playcount"]].describe().round(2))

st.markdown("""
Findings: The artist analytics set includes 2,378 artists with valid Last\\.fm engagement after removing records with missing listener data\\. Artist popularity exhibits an even more pronounced long\\-tail distribution than albums, with median listener counts \\(~7\\.7K\\) far below the mean \\(~193K\\), reflecting a small number of globally prominent artists alongside a large base of moderately known composers and performers\\. Log\\-transforming listener and playcount metrics meaningfully compresses this skew, producing more stable representations of artist popularity that are better suited for comparative analysis across roles, genres, and soundtrack participation\\.
""")

st.markdown("""
# IV\\. Transform and reduce tracks
""")

st.markdown("""
Let's now cleanup the tracks table and reduce it to the analytics set
""")

# =============================================================================
# IV. Transform and Reduce to Analytics Set (TRACKS)
# -----------------------------------------------------------------------------
# Goal:
#   Create tracks_analytics_df containing:
#     - Tracks belonging to a high-confidence soundtrack album universe
#       (canonical soundtrack + vote_count_above_500)
#     - Tracks with valid Last.fm playcount signal
#     - Log-transformed playcount for analysis
#
# Rationale:
#   - Tracks are analyzed independently from albums in terms of popularity.
#   - However, tracks must belong to albums that pass our soundtrack quality
#     gates to ensure a valid film→soundtrack context.
#   - Playcount is the primary engagement metric at the track level.
# =============================================================================


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def filter_tracks_to_album_universe(
    tracks_df: pd.DataFrame,
    albums_df: pd.DataFrame,
    album_key: str = "release_group_id"
) -> pd.DataFrame:
    """
    Restrict tracks to those belonging to albums that passed the
    canonical soundtrack + vote_count quality gate.

    Assumptions:
      - albums_df has already been filtered to canonical + vote_count_above_500.
      - album_key exists in both dataframes.
    """
    valid_album_ids = albums_df[album_key].unique()
    return tracks_df.loc[tracks_df[album_key].isin(valid_album_ids)].copy()


def qa_print_track_transform_stats(
    df_before: pd.DataFrame,
    df_after_album_filter: pd.DataFrame,
    df_after_playcount_clean: pd.DataFrame,
    playcount_col: str = "lfm_track_playcount"
) -> None:
    """
    Print lightweight QA stats for the track transformation.
    """
    print("TRACK ANALYTICS TRANSFORM — QA SUMMARY")
    print("-" * 60)

    print(f"Rows before:                         {len(df_before):,}")
    print(f"After album universe filter:         {len(df_after_album_filter):,}")
    print(f"After drop missing/0 playcounts:     {len(df_after_playcount_clean):,}")

    missing_pc = df_after_album_filter[playcount_col].isna().sum()
    zero_pc = (df_after_album_filter[playcount_col] == 0).sum()

    print("-" * 60)
    print(f"Playcount NaNs removed: {missing_pc:,}")
    print(f"Playcount zeros removed: {zero_pc:,}")

    if len(df_after_playcount_clean) > 0:
        print("-" * 60)
        print("Post-clean playcount sanity checks:")
        print(f"  min playcount: {df_after_playcount_clean[playcount_col].min():,}")
        print(f"  median:        {df_after_playcount_clean[playcount_col].median():,}")
        print(f"  max playcount: {df_after_playcount_clean[playcount_col].max():,}")
    print("-" * 60)
    print()

# -----------------------------------------------------------------------------
# Build tracks_analytics_df
# -----------------------------------------------------------------------------
tracks_before_df = tracks_df.copy()

# Step 1: Restrict to the canonical soundtrack album universe
tracks_album_filtered_df = filter_tracks_to_album_universe(
    tracks_before_df,
    albums_df=albums_filtered_df,  # from the album section (canonical + vote_count)
    album_key="release_group_id"
)

# Step 2: Drop tracks with no usable playcount signal
tracks_clean_df = drop_missing_or_nonpositive(tracks_album_filtered_df, "lfm_track_playcount")

# Step 3: Add log-transformed playcount
tracks_analytics_df = add_log_cols(
    tracks_clean_df,
    cols=["lfm_track_playcount"]
)

# QA printout
qa_print_track_transform_stats(
    df_before=tracks_before_df,
    df_after_album_filter=tracks_album_filtered_df,
    df_after_playcount_clean=tracks_analytics_df,
    playcount_col="lfm_track_playcount"
)

st.dataframe(tracks_analytics_df.head(3).round(2))
st.dataframe(tracks_analytics_df[["lfm_track_playcount","log_lfm_track_playcount"]].describe().round(2))

st.markdown("""
Findings: The track analytics set contains 32,261 tracks with valid Last\\.fm playcount, all belonging to canonical soundtrack albums for films that passed the vote\\-count quality gate\\. Track engagement is strongly right\\-skewed: while the median track has only a few hundred plays, a small number of standout tracks reach into the tens of millions\\. Applying a log transform substantially compresses this range, producing a more stable representation of track\\-level engagement that supports comparative analysis across films, albums, and soundtrack components in subsequent sections\\.
""")

st.markdown("""
# V\\. Transform and reduce wide
""")

st.markdown("""
Finally, let's handle the wide table
""")

# =============================================================================
# V. Transform and Reduce to Analytics Set (WIDE)
# -----------------------------------------------------------------------------
# Goal:
#   Create wide_analytics_df at track-grain, aligned to the exact same
#   universe as tracks_analytics_df.
#
# Universe definition (by design):
#   - Belongs to the canonical soundtrack + vote_count_above_500 album universe
#   - Has valid Last.fm track playcount signal
#
# Notes:
#   - We intentionally DO NOT filter wide rows based on artist-level Last.fm
#     completeness. Artist metrics are treated as enrichment and may be null.
#   - We DO log-transform all Last.fm numeric metrics in wide (track, album,
#     and artist), using a safe log to avoid -inf when values are missing/0.
# =============================================================================


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def filter_wide_to_track_universe(wide_df: pd.DataFrame, tracks_analytics_df: pd.DataFrame) -> pd.DataFrame:
    """
    Restrict wide_df to the track universe already defined by tracks_analytics_df.

    This is the cleanest way to guarantee that 'tracks_analytics_df' and
    'wide_analytics_df' represent the exact same set of track rows.
    """
    valid_track_ids = tracks_analytics_df["track_id"].unique()
    return wide_df.loc[wide_df["track_id"].isin(valid_track_ids)].copy()

def qa_print_wide_transform_stats(
    df_before: pd.DataFrame,
    df_after_track_filter: pd.DataFrame
) -> None:
    """
    Lightweight QA stats for wide transformation.
    """
    print("WIDE ANALYTICS TRANSFORM — QA SUMMARY")
    print("-" * 60)
    print(f"Rows before:                    {len(df_before):,}")
    print(f"After track universe filter:    {len(df_after_track_filter):,}")

    # Quick sanity checks for track playcount (the metric that defines the universe)
    if len(df_after_track_filter) > 0:
        print("-" * 60)
        print("Post-filter sanity checks (track playcount):")
        print(f"  track playcount min: {df_after_track_filter['lfm_track_playcount'].min():,}")
        print(f"  track playcount med: {df_after_track_filter['lfm_track_playcount'].median():,}")
        print(f"  track playcount max: {df_after_track_filter['lfm_track_playcount'].max():,}")

        # Enrichment completeness (informational only)
        album_listener_nulls = df_after_track_filter["lfm_album_listeners"].isna().sum()
        artist_listener_nulls = df_after_track_filter["lfm_album_primary_artist_listeners"].isna().sum()

        print("-" * 60)
        print("Enrichment completeness (not used for filtering):")
        print(f"  lfm_album_listeners nulls:                 {album_listener_nulls:,}")
        print(f"  lfm_album_primary_artist_listeners nulls:  {artist_listener_nulls:,}")

    print("-" * 60)
    print()

# -----------------------------------------------------------------------------
# Build wide_analytics_df
# -----------------------------------------------------------------------------
wide_before_df = wide_df.copy()

# Step 1: Filter to the same track universe as tracks_analytics_df
wide_filtered_df = filter_wide_to_track_universe(wide_before_df, tracks_analytics_df)

# Step 2: Add log transforms for all Last.fm metrics in wide
wide_analytics_df = add_log_cols(
    wide_filtered_df,
    cols=[
        "lfm_track_playcount", "lfm_track_listeners",
        "lfm_album_playcount", "lfm_album_listeners",
        "lfm_album_primary_artist_playcount", "lfm_album_primary_artist_listeners",
    ]
)

# QA printout
qa_print_wide_transform_stats(
    df_before=wide_before_df,
    df_after_track_filter=wide_analytics_df
)

st.dataframe(wide_analytics_df.head(3).round(2))

display(
    wide_analytics_df[
        [
            "lfm_track_playcount", "log_lfm_track_playcount",
            "lfm_track_listeners", "log_lfm_track_listeners",
            "lfm_album_playcount", "log_lfm_album_playcount",
            "lfm_album_listeners", "log_lfm_album_listeners",
            "lfm_album_primary_artist_playcount", "log_lfm_album_primary_artist_playcount",
            "lfm_album_primary_artist_listeners", "log_lfm_album_primary_artist_listeners",
        ]
    ].describe().round(2)
)

st.markdown("""
Findings:  The wide analytics set contains 32,261 track\\-level rows, aligned exactly to the validated track universe derived from canonical soundtrack albums and films that passed the vote\\-count quality gate\\. Track playcount in the wide table mirrors the track\\-only analytics set, with a pronounced right skew driven by a small number of highly popular tracks\\. 
""")

st.markdown("""
To make these heavy\\-tailed engagement signals more analysis\\-ready, we log\\-transform all available Last\\.fm metrics in the wide table \\(track, album, and album\\-primary\\-artist listeners/playcounts\\)\\. This stabilizes distributions for downstream comparisons while preserving the full film, album, and artist context—and it does so without imposing additional filtering on enrichment fields, which may be null when upstream Last\\.fm data is missing\\.
""")

st.markdown("""
# VI\\. Write to file
""")

# =============================================================================
# Persist Analytics Sets to Disk
# -----------------------------------------------------------------------------
# We write each analytics-ready dataframe to disk so downstream analysis
# notebooks can load a stable, frozen version of the 4.7 outputs.
#
# Files written:
#   ./pipeline/4.7.albums_analytics_set.csv
#   ./pipeline/4.7.artists_analytics_set.csv
#   ./pipeline/4.7.tracks_analytics_set.csv
#   ./pipeline/4.7.wide_analytics_set.csv
# =============================================================================

OUTPUT_DIR = "./pipeline"

albums_analytics_df.to_csv(
    f"{OUTPUT_DIR}/4.7.Albums_analytics_set.csv",
    index=False
)

artists_analytics_df.to_csv(
    f"{OUTPUT_DIR}/4.7.Artists_analytics_set.csv",
    index=False
)

tracks_analytics_df.to_csv(
    f"{OUTPUT_DIR}/4.7.Tracks_analytics_set.csv",
    index=False
)

wide_analytics_df.to_csv(
    f"{OUTPUT_DIR}/4.7.Wide_analytics_set.csv",
    index=False
)

print("4.7 analytics sets successfully written to disk.")
