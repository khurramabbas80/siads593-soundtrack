import streamlit as st
import os, sys

st.set_page_config(page_title="5.4a Audio Features Cleaning", layout="wide")

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

# Standard library imports
import os
from typing import List, Union

# Third-party imports
import numpy as np
import pandas as pd

# Load the dataframes
tracks_df = pd.read_csv("./pipeline/4.7.Tracks_analytics_set.csv")
wide_df = pd.read_csv("./pipeline/4.7.Wide_analytics_set.csv")

print(f"Total records in tracks_df: {tracks_df.shape[0]}")
print(tracks_df.columns)
tracks_df.head()

st.markdown("""
# II\\. Check for Duplicate Spotify Track ID's
""")

st.markdown("""
During the merging of the API dumps from the Spotify and Soundnet APIs, we noticed that there were some instances where Spotify Tack ID's were showing up more than once, sometimes with records that had different track titles\\. Below, we set out to investigate this further\\. 
""")

# Check for duplicates in spotify_track_id

duplicate_row_count = tracks_df["spotify_track_id"].duplicated(keep=False).sum()
# auto-detected possible Altair chart: duplicate_row_count
try:
    st.altair_chart(duplicate_row_count, use_container_width=True)
except Exception:
    st.write(duplicate_row_count)

# Inspect the rows

duplicate_rows_df = (
    tracks_df
    .loc[tracks_df["spotify_track_id"].duplicated(keep=False)]
    .sort_values("spotify_track_id")
)

# auto-detected possible Altair chart: duplicate_rows_df
try:
    st.altair_chart(duplicate_rows_df, use_container_width=True)
except Exception:
    st.write(duplicate_rows_df)

st.markdown("""
As track\\_id is a unique identifier in the source data that preceded the Spotify API pulls, we wanted to do a closer inspection of records where spotify\\_track\\_id's were assigned to different track\\_id's\\.
""")

# ------------------------------------------------------------
# STEP 1: Identify Spotify IDs that map to multiple track_ids
# ------------------------------------------------------------

spotify_id_multi_track = (
    tracks_df
    # Group rows by spotify_track_id
    .groupby("spotify_track_id")["track_id"]

    # Count the number of DISTINCT track_id values per spotify ID
    # (nunique = number of unique values)
    .nunique()

    # Convert the resulting Series back into a dataframe
    .reset_index(name="distinct_track_ids")

    # Keep only Spotify IDs that map to MORE THAN ONE track_id
    # These represent potential ID collisions
    .query("distinct_track_ids > 1")
)


# ------------------------------------------------------------
# STEP 2: Flag all rows involved in those collisions
# ------------------------------------------------------------

tracks_df["spotify_track_id_collision"] = (
    tracks_df["spotify_track_id"]

    # Check whether each row's spotify_track_id
    # appears in the list of problematic IDs
    .isin(spotify_id_multi_track["spotify_track_id"])
)

# This creates a new Boolean column:
#   True  = this row is part of a Spotify ID collision
#   False = this row is not part of a collision


# ------------------------------------------------------------
# STEP 3: Extract only the rows involved in collisions
# ------------------------------------------------------------

collision_rows_df = (
    tracks_df

    # Keep only rows where collision flag is True
    .loc[tracks_df["spotify_track_id_collision"]]

    # Sort for readability:
    # First by spotify ID, then by track_id
    .sort_values(["spotify_track_id", "track_id"])
)

# auto-detected possible Altair chart: collision_rows_df
try:
    st.altair_chart(collision_rows_df, use_container_width=True)
except Exception:
    st.write(collision_rows_df)

st.markdown("""
Here we noticed that there were some instances where the same spotify\\_track\\_id was being returned for two tracks with similar, but not identical titles \\(e\\.g\\., Space Gate, Assault at the Space Gate\\)\\. Fortunately, when we got the results from the Spotify API, we stored the track titles that Spotify was attempting to match to\\. We will merge those in from the API result CSVs in order to compare those\\.
""")

st.markdown("""
# III\\. Compare Mis\\-matched Track Titles
""")

# Load CSV from first Spotify API run

filepath_first_run = '/work/api_dumps/pycharm_spotify_id_lookup_results.csv'
df_first_run = pd.read_csv(filepath_first_run)

print(f"Total records in 1st API run: {df_first_run.shape[0]}")
df_first_run.head()

# Load CSV from the second Spotify API run

filepath_second_run = '/work/api_dumps/scoped_spotify_id_lookup_results.csv'
df_spotify_second_run = pd.read_csv(filepath_second_run)

print(f"Number of records: {len(df_spotify_second_run)}")
df_spotify_second_run.head()

# Helper function to concatenate two dataframes and return the unique rows
# based on a specified column

def combine_unique_rows(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    unique_col: str,
    columns_to_keep: List[str],
) -> pd.DataFrame:
    """
    Combine two DataFrames and return a DataFrame containing only unique rows
    based on a specified column.

    Rows where `unique_col` is NaN are dropped before de-duplication.
    Missing columns in either input DataFrame are created and filled with NaN.
    """

    # ------------------------------------------------------------
    # STEP 1: Validate inputs
    # ------------------------------------------------------------

    # Ensure that the column used to determine uniqueness
    # is actually included in the output columns.
    # Without it, we could not properly de-duplicate.
    if unique_col not in columns_to_keep:
        raise ValueError(
            f"`unique_col` ('{unique_col}') must be included in columns_to_keep."
        )

    # ------------------------------------------------------------
    # STEP 2: Concatenate the two input DataFrames
    # ------------------------------------------------------------

    # Stack the two DataFrames vertically (row-wise).
    # ignore_index=True resets the row index so it runs 0...N-1
    combined_df = pd.concat([df_left, df_right], ignore_index=True)

    # ------------------------------------------------------------
    # STEP 3: Ensure all requested output columns exist
    # ------------------------------------------------------------

    # If a column listed in `columns_to_keep` does not exist
    # in either df_left or df_right, create it and fill with NaN.
    # This guarantees a consistent schema in the final output.
    for col in columns_to_keep:
        if col not in combined_df.columns:
            combined_df[col] = pd.NA

    # ------------------------------------------------------------
    # STEP 4: Clean and de-duplicate
    # ------------------------------------------------------------

    combined_unique_df = (
        combined_df

        # Drop rows where the uniqueness column is missing.
        # We cannot determine uniqueness if the key is NaN.
        .dropna(subset=[unique_col])

        # Keep only the requested columns (and enforce column order).
        .loc[:, columns_to_keep]

        # Remove duplicate rows based on `unique_col`.
        # keep="first" preserves the first occurrence encountered.
        .drop_duplicates(subset=unique_col, keep="first")

        # Reset the index for a clean, sequential index.
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------
    # STEP 5: Return result
    # ------------------------------------------------------------

    return combined_unique_df

# Concatenate the two API result sets and de-duplicate on Spotify ID

cols_to_keep = ['spotify_track_id', 'matched_track_name', 'matched_artists', 'matched_album']

spotify_api_matches_df = combine_unique_rows(
    df_left=df_first_run,
    df_right=df_spotify_second_run,
    unique_col="spotify_track_id",
    columns_to_keep=cols_to_keep
)

print(f"Number of records in combined df: {len(spotify_api_matches_df)}")
spotify_api_matches_df.head()

# Validate there are no nulls/blanks for spotify_track_id

assert (
    spotify_api_matches_df["spotify_track_id"].notna()
    & (spotify_api_matches_df["spotify_track_id"].astype(str).str.strip() != "")
).all(), "spotify_track_id contains null or blank values"

# Helper function to join two dataframes, with de-dupe

def join_with_dedup(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    on: Union[str, List[str]],
    how: str = "left",
    columns_to_add: Union[List[str], None] = None,
    suffixes: tuple = ("", "_right"),
) -> pd.DataFrame:
    """
    Join two DataFrames with de-duplication on the right DataFrame.

    Parameters
    ----------
    left_df : pd.DataFrame
        Left DataFrame.
    right_df : pd.DataFrame
        Right DataFrame (will be de-duplicated before join).
    on : str or list of str
        Column(s) to join on.
    how : str, default "left"
        Type of join: 'left', 'right', 'inner', 'outer'.
    columns_to_add : list of str or None, default None
        Columns from right_df to bring in. If None, brings all columns.
        Join keys are automatically included.
    suffixes : tuple, default ("", "_right")
        Suffixes to apply to overlapping column names.

    Returns
    -------
    pd.DataFrame
        Joined DataFrame.
    """

    # Get dataframe length before merging
    orig_count = len(left_df)

    # Normalize join keys
    on_cols = [on] if isinstance(on, str) else list(on)

    # Determine columns to keep from right_df
    if columns_to_add is None:
        right_subset = right_df.copy()
    else:
        cols = list(set(on_cols + columns_to_add))
        right_subset = right_df[cols].copy()

    # De-duplicate right dataframe on join keys
    right_deduped = (
        right_subset
        .sort_values(on_cols)   # deterministic behavior
        .drop_duplicates(subset=on_cols, keep="first")
    )

    # Perform join
    joined_df = left_df.merge(
        right_deduped,
        on=on_cols,
        how=how,
        suffixes=suffixes
    )

    print(f"Number of records before merging: {orig_count}")
    print(f"Number of records after merging: {len(joined_df)}")
    return joined_df

# Merge the Spotify API track, artist and album name matches into the tracks dataframe

cols = ['matched_track_name', 'matched_artists', 'matched_album']

tracks_spotify_api_matches_df = join_with_dedup(
    left_df=tracks_df,
    right_df=spotify_api_matches_df,
    on=["spotify_track_id"],
    how="left",
    columns_to_add=cols
)

tracks_spotify_api_matches_df.head()

# Check that dataframe lengths match

assert len(tracks_df) == len(tracks_spotify_api_matches_df)

st.markdown("""
We created the function below to normalize track titles for comparison\\. For example, we noticed some track titles across columns were the same except for "\\(Album Mix\\)\\."
""")

# Helper function to normalize track titles for better comparison

def normalize_title(s):
    """
    Normalize track titles for comparison by:
    - converting to string
    - trimming whitespace
    - lowercasing
    - collapsing repeated whitespace
    - removing dash-based subtitles (both standalone and in parentheses)
    - removing any 'from ...' clause following (, -, –, or —
    - removing all punctuation
    """

    return (
        s.astype(str)
         # basic normalization
         .str.strip()
         .str.lower()
         .str.replace(r"\s+", " ", regex=True)

         # remove parenthetical content ONLY if it contains a dash-subtitle keyword
         # e.g. "(Live)", "(Remastered 2011)", "(Album Mix)"
         .str.replace(
             r"\s*\(\s*(live|remaster(ed)?|album mix|single version)[^)]*\)",
             "",
             regex=True,
         )

         # remove dash-based subtitles outside parentheses
         .str.replace(
             r"\s*[-–—]\s*(live|remaster(ed)?|album mix|single version).*",
             "",
             regex=True,
         )

         # remove "from ..." clauses following (, -, –, —
         .str.replace(
             r"\s*[\(\-–—]\s*from\s+.*$",
             "",
             regex=True,
         )

         # remove all punctuation
         .str.replace(r"[^\w\s]", "", regex=True)

         # final cleanup
         .str.replace(r"\s+", " ", regex=True)
         .str.strip()
    )

# Identify duplicate Spotify ID's

duplicated_spotify_ids = (
    tracks_spotify_api_matches_df["spotify_track_id"]
    .value_counts()
    .loc[lambda x: x > 1]
    .index
)

# Inspect rows where the normalized titles do not match

problem_rows_df = (
    tracks_spotify_api_matches_df
    .loc[
        tracks_spotify_api_matches_df["spotify_track_id"].isin(duplicated_spotify_ids)
        & (
            normalize_title(tracks_spotify_api_matches_df["track_title_cleaned"])
        )
    ]
    .sort_values(["spotify_track_id", "track_title_cleaned"])
)

cols_to_view = [
    "track_id",
    "spotify_track_id",
    "track_title_cleaned",
    "matched_track_name",
    "recording_artist_credit",
    "film_title"
]

problem_rows_df[cols_to_view]

# Flag the false positives

false_positive_mask = (
    tracks_spotify_api_matches_df["spotify_track_id"].isin(duplicated_spotify_ids)
    & (
        normalize_title(tracks_spotify_api_matches_df["track_title_cleaned"])
    )
)

tracks_spotify_api_matches_df["spotify_api_false_positive"] = false_positive_mask
tracks_spotify_api_matches_df.head()

st.markdown("""
For the false positives, we will clear the values that the Spotify API filled so that we don't feed inaccurate data into our visuals and analysis\\.
""")

# Replace false positives with "NA" values

cols_to_clear = [
    "spotify_track_id",
    "spotify_url",
    "key",
    "mode",
    "camelot",
    "tempo",
    "duration",
    "popularity",
    "energy",
    "danceability",
    "happiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "speechiness",
    "loudness",
    "spotify_popularity",
]

tracks_spotify_api_matches_df.loc[
    tracks_spotify_api_matches_df["spotify_api_false_positive"],
    cols_to_clear
] = pd.NA

tracks_spotify_api_matches_df.loc[
    tracks_spotify_api_matches_df["spotify_api_false_positive"],
    cols_to_clear
].head()

st.markdown("""
# V\\. Merge Additional Features for Exploration
""")

# Bring composer_primary_clean into our dataframe from the wide table

tracks_spotify_api_matches_df = join_with_dedup(
    left_df=tracks_spotify_api_matches_df,
    right_df=wide_df,
    on=["track_id"],
    how="left",
    columns_to_add=["composer_primary_clean"]
)

tracks_spotify_api_matches_df.head()

# Bring film genres into our dataframe from the wide table

cols_to_add = [
    'film_is_action',
    'film_is_adventure',
    'film_is_animation',
    'film_is_comedy',
    'film_is_crime',
    'film_is_documentary',
    'film_is_drama',
    'film_is_family',
    'film_is_fantasy',
    'film_is_history',
    'film_is_horror',
    'film_is_music',
    'film_is_mystery',
    'film_is_romance',
    'film_is_science_fiction',
    'film_is_tv_movie',
    'film_is_thriller',
    'film_is_war',
    'film_is_western'
    ]

tracks_spotify_api_matches_df = join_with_dedup(
    left_df=tracks_spotify_api_matches_df,
    right_df=wide_df,
    on=["tmdb_id"],
    how="left",
    columns_to_add=cols_to_add
)

tracks_spotify_api_matches_df.head()

st.markdown("""
# IV\\. Drop NA's and Missing Values
""")

# Get info on the columns with nulls

tracks_spotify_api_matches_df.info()

st.markdown("""
There are several columns in the dataframe that are either duplicative or that we don't think will be interesting for track level visuals and analysis, so these will be dropped\\.
""")

cols_to_drop = [
    "composer_names_text",
    "release_group_id",
    "release_id",
    "match_method",
    "album_us_release_date",
    "us_date_has_missing_month",
    "us_date_has_missing_day",
    "medium_id",
    "disc_number",
    "medium_format",
    "recording_id",
    "recording_mbid",
    "recording_title",
    "recording_length_ms",
    "recording_first_release_year",
    "recording_first_release_month",
    "recording_first_release_day",
    "isrcs_text",
    "recording_tags_text",
    "work_ids_text",
    "work_titles_text",
    "lyricist_names_text",
    "lfm_track_status",
    "lfm_track_url",
    "lfm_track_query_method",
    "lfm_track_pulled_at",
    "spotify_track_id_collision",
    "matched_artists",
    "matched_album",
    "matched_track_name",
    "spotify_api_false_positive",
    "track_length_ms",
    "popularity",
    "duration"
]

tracks_spotify_api_matches_df = tracks_spotify_api_matches_df.drop(
    columns=cols_to_drop,
    errors="ignore"  # prevents error if any column is already missing
)

# For track_title_cleaned NA's (there are only 2), we will just fill with the values in track_title

tracks_spotify_api_matches_df["track_title_cleaned"] = (
    tracks_spotify_api_matches_df["track_title_cleaned"]
    .fillna(tracks_spotify_api_matches_df["track_title"])
)

tracks_spotify_api_matches_df["track_title_cleaned"].isna().sum()

# Re-check for nulls with narrower column universe

tracks_spotify_api_matches_df.info()

st.markdown("""
We still have a little over 3k nulls for each of our audio features, which will be the main focus of our exploration\\. These would have either been due to song names that didn't hit on the Spotify API, false positives, or various API errors when querying the Soundnet API\\. In the interest of preserving the variation in these columns for later analysis, we will drop the rows with null values\\.
""")

# Helper function to drop nulls or blanks for a specified set of columns

def drop_null_or_blank_rows(
    df: pd.DataFrame,
    cols: list | None = None
) -> pd.DataFrame:
    """
    Drop rows that contain any null or blank (empty or whitespace-only) values
    in specified columns (or all columns if none specified), and print a summary.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    cols : list, optional
        List of column names to check. If None, all columns are checked.

    Returns
    -------
    pd.DataFrame
        A new dataframe with rows containing nulls or blanks removed.
    """

    # ------------------------------------------------------------
    # 1️⃣ Determine which columns we will evaluate
    # ------------------------------------------------------------
    # If no specific columns were provided, check all columns
    if cols is None:
        cols = df.columns

    # Count rows before cleaning
    n_before = len(df)

    # Create a temporary dataframe containing only the columns
    # we want to check (this does NOT modify the original df)
    subset_df = df[cols]

    # ------------------------------------------------------------
    # 2️⃣ Build a row-level mask for null or blank values
    # ------------------------------------------------------------
    # Step A: subset_df.isna()
    #   -> True where values are actual NaN / None
    #
    # Step B: subset_df.astype(str).apply(lambda col: col.str.strip().eq(""))
    #   -> Converts values to string
    #   -> Removes leading/trailing whitespace
    #   -> Checks whether the result is an empty string ""
    #
    # Step C: Combine A and B with OR (|)
    #   -> True if value is null OR blank
    #
    # Step D: .any(axis=1)
    #   -> Collapse across columns
    #   -> True if ANY checked column in that row is null/blank
    mask_any_null_or_blank = (
        subset_df.isna()
        | subset_df.astype(str).apply(lambda col: col.str.strip().eq(""))
    ).any(axis=1)

    # Count how many rows will be dropped
    n_dropped = mask_any_null_or_blank.sum()

    # ------------------------------------------------------------
    # 3️⃣ Keep only rows that are NOT flagged
    # ------------------------------------------------------------
    # ~mask_any_null_or_blank means:
    #   Keep rows where condition is False
    df_clean = df.loc[~mask_any_null_or_blank].copy()

    # Count rows after cleaning
    n_after = len(df_clean)

    # ------------------------------------------------------------
    # 4️⃣ Print summary
    # ------------------------------------------------------------
    print(f"Columns checked: {list(cols)}")
    print(f"Input records:   {n_before}")
    print(f"Records dropped: {n_dropped}")
    print(f"Output records:  {n_after}")

    return df_clean

# Drop nulls for the audio feature columns

audio_cols = [
    'key',
    'mode',
    'camelot',
    'tempo',
    'energy',
    'danceability',
    'happiness',
    'acousticness',
    'instrumentalness',
    'liveness',
    'speechiness',
    'loudness'
]

tracks_clean_df = drop_null_or_blank_rows(
    tracks_spotify_api_matches_df,
    cols=audio_cols
)

tracks_clean_df.info()

st.markdown("""
That appeared to have resolved any remaining nulls in the dataframe, but we noticed that loudness should be numeric, not object\\.
""")

# Convert loudness to numeric

tracks_clean_df["loudness"] = (
    tracks_clean_df["loudness"]
    .str.replace(" dB", "", regex=False)
    .astype(float)
)

print(f"Total records after cleaning: {len(tracks_clean_df)}")
tracks_clean_df[audio_cols].head()

st.markdown("""
# VI\\. Write to File
""")

# Save cleaned track file to CSV

out_path = "./pipeline/5.4.a.Tracks_clean.csv"

tracks_clean_df.to_csv(out_path, index=False)
