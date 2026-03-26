import streamlit as st
import os, sys

st.set_page_config(page_title="3.2 Remove unofficial albums", layout="wide")

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
# I\\. Setup and Album Title Inspection
""")

# Standard library imports
import os
from datetime import datetime

# Third-party imports
import numpy as np
import pandas as pd

# SETUP & CONFIGURATION
# os.chdir("/work")  # path adjusted for Streamlit
print(os.listdir("./pipeline"))

# Show all columns (no truncation)
pd.set_option("display.max_columns", None)

# Load the albums dataframe

album_df = pd.read_csv("./pipeline/2.1.MUSICBRAINZ_mv_tmdb_soundtrack_album_spine_2015_2025.csv")

# Display initial data inspection
st.dataframe(album_df.head())
st.dataframe(album_df.columns)

st.markdown("""
# II\\. Unofficial Album Exploration
""")

# =========================
# Inspect + summarize "unofficial" albums (by release_status)
# Assumes: album_df exists
# =========================

# 1) Quick coverage: how many rows + how many unique release_groups by status?
status_cols = ["release_status", "release_group_mbid", "album_title", "tmdb_id", "film_title", "film_year", "album_us_release_date"]

# Make sure the key columns exist (won't error if some are missing)
status_cols = [c for c in status_cols if c in album_df.columns]

print("Rows in album_df:", len(album_df))
print("\nRelease status distribution (rows):")
st.dataframe(album_df["release_status"].fillna("(missing)").value_counts(dropna=False))

# Unique release_groups per status
rg_per_status = (
    album_df.assign(release_status=album_df["release_status"].fillna("(missing)"))
           .groupby("release_status")["release_group_mbid"]
           .nunique()
           .sort_values(ascending=False)
           .to_frame("unique_release_groups")
)
print("\nRelease status distribution (unique release_group_mbid):")
st.write(rg_per_status)

# 2) Define what's "unofficial" (everything except Official)
album_df["release_status_clean"] = album_df["release_status"].fillna("(missing)")
unofficial_df = album_df.loc[album_df["release_status_clean"] != "Official"].copy()

print("\nUnofficial rows:", len(unofficial_df))
print("Unofficial unique release_group_mbids:", unofficial_df["release_group_mbid"].nunique())
print("Unofficial unique tmdb_ids:", unofficial_df["tmdb_id"].nunique())

# 3) Breakdown of unofficial types (rows + unique release_groups)
unofficial_breakdown = (
    unofficial_df.groupby("release_status_clean")
                 .agg(
                     row_ct=("release_group_mbid", "size"),
                     unique_release_groups=("release_group_mbid", "nunique"),
                     unique_tmdb_ids=("tmdb_id", "nunique"),
                 )
                 .sort_values(["unique_release_groups", "row_ct"], ascending=False)
)
print("\nUnofficial breakdown:")
st.write(unofficial_breakdown)

st.markdown("""
Findings: This breakdown confirms that the vast majority of records in the MusicBrainz export are standard releases, with only a small fraction flagged as bootlegs, promotions, withdrawn titles, or pseudo\\-releases\\. In other words, edge\\-case release types exist, but they are not materially driving the dataset\\. That gives us confidence that our album spine is largely composed of legitimate commercial soundtrack releases rather than archival artifacts or non\\-standard entries\\.
""")

# 4) How much would we drop if we filter them out?
total_rgs = album_df["release_group_mbid"].nunique()
unofficial_rgs = unofficial_df["release_group_mbid"].nunique()

print("\nImpact if we remove unofficial:")
print(f"- % of rows dropped: {len(unofficial_df) / len(album_df):.2%}")
print(f"- % of unique release_groups dropped: {unofficial_rgs / total_rgs:.2%}")

# 5) Spot-check: show a few examples per unofficial status
# (useful for sanity-checking what kinds of albums these are)
cols_for_view = ["release_status_clean", "album_title", "release_group_mbid", "rg_primary_type",
                            "album_us_release_date", "release_status", "release_packaging",
                            "rg_tags_text", "label_names", "match_method",
                            "film_title", "film_year", "tmdb_id"
                            ]

for status in unofficial_df["release_status_clean"].value_counts().index:
    print(f"\n--- Examples: {status} ---")
    display(
        unofficial_df.loc[unofficial_df["release_status_clean"] == status, cols_for_view]
                    .drop_duplicates(subset=[c for c in ["release_group_mbid", "tmdb_id"] if c in unofficial_df.columns])
                    .head(15)
    )



st.markdown("""
Findings: A closer look at non\\-standard release\\_status categories shows that they are heterogeneous and inconsistently applied\\. Many “\\(missing\\)” entries correspond to legitimate soundtracks, while “Promotion,” “Withdrawn,” and “Pseudo\\-Release” statuses often reflect distribution variants, alternate\\-language representations, or release\\-state artifacts rather than distinct soundtrack concepts\\. Because these categories blur structural boundaries at the release\\_group level, we opted to anchor the analytical spine exclusively to “Official” releases\\. This ensures a clean, reproducible album universe, even if it excludes some edge\\-case but potentially valid variants\\.
""")

# 6) Identify the "worst offenders" (unofficial release_groups linked to many films)
# (if you still have dupes around, this can show where unofficial data may be muddying links)
if {"release_group_mbid", "tmdb_id"}.issubset(unofficial_df.columns):
    unofficial_rg_multi = (
        unofficial_df.groupby("release_group_mbid")["tmdb_id"]
                     .nunique()
                     .sort_values(ascending=False)
    )
    print("\nUnofficial release_groups mapped to multiple tmdb_ids (top 20):")
    st.write(unofficial_rg_multi.head(20).to_frame("distinct_tmdb_ids"))

    # Show context for the top few
    top_unofficial_rgs = unofficial_rg_multi.head(5).index.tolist()
    print("\nContext rows for top unofficial dupes (top 5 release_groups):")
    display(
        unofficial_df.loc[unofficial_df["release_group_mbid"].isin(top_unofficial_rgs), cols_for_view]
                    .sort_values(["release_group_mbid"] + (["film_year"] if "film_year" in cols_for_view else []))
    )

st.markdown("""
Findings: A deeper dive into unofficial release\\_groups reveals a more serious issue: several are linked to multiple distinct TMDB films, often due to generic titles \\(e\\.g\\., Limbo, Inside, Rage\\) and title\\-based matching\\. In some cases, a single release\\_group is mapped to as many as five different films\\. This many\\-to\\-many ambiguity confirms that non\\-Official statuses introduce structural contamination into the film–album spine\\. Restricting the dataset to Official releases is therefore not just a cleanliness preference, but a necessary step to prevent cross\\-film linkage errors\\.
""")

st.markdown("""
# III\\. Deleting it from relevant tables
""")

st.markdown("""
### III\\.1 Cleaning up the Album DataFrame
""")

st.markdown("""
After confirming that non\\-Official release groups introduce cross\\-film ambiguity, the next step was to formalize their removal\\. Rather than filtering implicitly downstream, we construct an explicit deletion list at the release\\_group level\\. This allows us to surgically remove problematic film–album links while preserving a clear audit trail of what was excluded and why\\.
""")

# 7) Create a deletion list you can apply later (wide_df, etc.)
# Each is a tuple: (release_group_mbid, tmdb_id)
unofficial_removed_links_rgid = list(
    unofficial_df.loc[:, ["release_group_id", "tmdb_id"]]
                    .dropna()
                    .drop_duplicates()
                    .itertuples(index=False, name=None) # Yields plain tuples of (release_group_id, tmdb_id) without index
)

# For these deletes, we only really need the release group id
unofficial_rgid_set = set(rg_id for rg_id, _tmdb in unofficial_removed_links_rgid)

print("Deletion list (release_group_id, tmdb_id):", len(unofficial_removed_links_rgid))
print("Unique release_group_id to delete from tracks:", len(unofficial_rgid_set))
print(unofficial_removed_links_rgid[:10])

st.markdown("""
Findings: This process identified 173 film–album link pairs corresponding to 143 unique release\\_groups marked as non\\-Official\\. In practical terms, these represent structurally ambiguous or non\\-canonical soundtrack mappings that would otherwise propagate into the track\\-level dataset\\. By isolating these release\\_groups early, we prevent cross\\-film contamination in the wide analytical layer and ensure that downstream listener metrics are attributed only to vetted, Official soundtrack albums\\.
""")

official_album_df = album_df[album_df["release_status_clean"] == "Official"].copy()

print("Rows before:", len(album_df))
print("Rows after (Official only):", len(official_album_df))

print("\nRelease status distribution (official df):")
st.dataframe(official_album_df["release_status_clean"].value_counts(dropna=False))

official_album_df.to_csv(
    "./pipeline/3.2.Albums_official_df.csv",
    index=False
)

st.markdown("""
Findings: Filtering to release\\_status\\_clean == "Official" reduced the album layer from 5,209 to 5,036 rows — a net removal of 173 records\\. This confirms that non\\-Official releases represented a relatively small but non\\-trivial portion of the dataset \\(~3%\\)\\. After filtering, the album spine consists entirely of Official releases, providing a cleaner and more defensible foundation for downstream track\\- and listener\\-level analysis\\.
""")

set(unofficial_rgid_set) - set(album_df.loc[album_df["release_status_clean"] != "Official", "release_group_id"])

st.markdown("""
Findings: A sanity check confirmed that the set of release\\_group IDs flagged as unofficial aligns perfectly with the rows removed by filtering to release\\_status\\_clean == "Official" \\(empty set difference\\)\\. In other words, the applied filter and the earlier diagnostic deletion list are fully consistent\\. This confirms that no unofficial release groups remain in the album spine and that the QA logic is internally coherent\\.
""")

st.markdown("""
### III\\.2 Cleaning up the Track DataFrame
""")

st.markdown("""
Now, let's use the deletion list constructed in III\\.1 to identify records to purge from the other tables\\.
""")

# Load the tracks dataframe

tracks_df = pd.read_csv("./pipeline/2.2.MUSICBRAINZ_mv_tmdb_soundtrack_track_spine_2015_2025.csv")
st.dataframe(tracks_df.columns)
st.dataframe(tracks_df.head())


# Calculate the length BEFORE the deletion
tracks_before = len(tracks_df)

# Delete rows that contain the release group ids in the delete list
is_unofficial_track = tracks_df["release_group_id"].isin(unofficial_rgid_set)
deleted_track_ct = int(is_unofficial_track.sum())

tracks_official_df = tracks_df.loc[~is_unofficial_track].copy()

print(f"Tracks before: {tracks_before:,}")
print(f"Tracks deleted: {deleted_track_ct:,}")
print(f"Tracks after:  {len(tracks_official_df):,}")

tracks_official_df.to_csv(
    "./pipeline/3.2.Tracks_official_df.csv",
    index=False
)

# For completeness, let's upload artist_df even though we don't need to deletion on it and save the output into
# the pipeline

artist_df = pd.read_csv("./pipeline/2.2.MUSICBRAINZ_mv_tmdb_soundtrack_artist_spine_2015_2025.csv")
artist_df.to_csv(
    "./pipeline/3.2.Artists_official_df.csv",
    index=False
)

st.markdown("""
### III\\.3 Cleaning up the Wide DataFrame
""")

# Load the wide dataframe

wide_df = pd.read_csv("./pipeline/2.2.MUSICBRAINZ_mv_tmdb_soundtrack_wide_track_2015_2025.csv")
st.dataframe(wide_df.columns)
st.dataframe(wide_df.head())


# Calculate the length BEFORE the deletion
wide_before = len(wide_df)

# Delete rows that contain the release group ids in the delete list
is_unofficial_wide = wide_df["release_group_id"].isin(unofficial_rgid_set)
deleted_wide_ct = int(is_unofficial_wide.sum())

wide_official_df = wide_df.loc[~is_unofficial_wide].copy()

print(f"Records before: {wide_before:,}")
print(f"Records deleted: {deleted_wide_ct:,}")
print(f"Records after:  {len(wide_official_df):,}")

wide_official_df.to_csv(
    "./pipeline/3.2.Wide_official_df.csv",
    index=False
)
