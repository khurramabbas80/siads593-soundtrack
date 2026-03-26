import streamlit as st
import os, sys

st.set_page_config(page_title="4.5 Join everything", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# I\\. Setup and load all dataframes
""")

# Standard library imports
import os

# Third-party imports
import numpy as np
import pandas as pd

# Load the awards dataframes
albums_awards_df = pd.read_csv("./pipeline/4.1.1.Albums_awards_appended.csv")
artists_awards_df = pd.read_csv("./pipeline/4.1.1.Artists_awards_appended.csv")
tracks_awards_df = pd.read_csv("./pipeline/4.1.1.Tracks_awards_appended.csv")
wide_awards_df = pd.read_csv("./pipeline/4.1.1.Wide_awards_appended.csv")

# Load the last.fm dataframes

albums_lfm_df = pd.read_csv("./pipeline/4.2.Albums_lastfm_appended.csv")
artists_lfm_df = pd.read_csv("./pipeline/4.2.Artists_lastfm_appended.csv")
tracks_lfm_df = pd.read_csv("./pipeline/4.2.Tracks_lastfm_appended.csv")
wide_lfm_df = pd.read_csv("./pipeline/4.2.Wide_lastfm_appended.csv")

# Load the audio features dataframes

albums_audio_df = pd.read_csv("./pipeline/4.4.Albums_rapid_api_pull.csv")
artists_audio_df = pd.read_csv("./pipeline/4.4.Artists_rapid_api_pull.csv")
tracks_audio_df = pd.read_csv("./pipeline/4.4.Tracks_rapid_api_pull.csv")
wide_audio_df = pd.read_csv("./pipeline/4.4.Wide_rapid_api_pull.csv")

st.markdown("""
We have to be surgically careful with all our merges\\.\\.\\. Let's use a function to control the merge\\.
""")

st.markdown("""
# II\\. Merge helper function
""")

def merge_layer(left_df, right_df, keys, right_name):
    """
    Merge right_df into left_df at the given key grain, without row multiplication.
    - Keeps all rows from left_df.
    - Adds only columns that are not already in left_df (besides keys).
    - Validates that right_df is unique on keys (1:1).
    """
    # Ensure right is unique on keys (otherwise merge can multiply rows)
    right_unique = right_df.drop_duplicates(subset=keys).copy()

    # Only bring in truly new columns from the right side
    new_cols = [c for c in right_unique.columns if c not in left_df.columns or c in keys]
    right_unique = right_unique[new_cols]

    out = left_df.merge(
        right_unique,
        on=keys,
        how="left",
        validate="1:1"  # many rows on left allowed; right must be unique
    )

    print(f"[{right_name}] merged on {keys}: {left_df.shape} -> {out.shape} (added cols: {len(new_cols) - len(keys)})")
    return out

st.markdown("""
# III\\. Album dataframe merge
""")

st.markdown("""
Let's merge the album dataframes
""")

# ------------------------------------------------------------
# 1) Pre-merge uniqueness checks at album grain
# ------------------------------------------------------------
KEYS = ["tmdb_id", "release_group_id"]
dup_awards = albums_awards_df.duplicated(subset=KEYS).sum()
dup_lfm    = albums_lfm_df.duplicated(subset=KEYS).sum()

print("Awards duplicate album keys:", int(dup_awards))
print("Last.fm duplicate album keys:", int(dup_lfm))

assert dup_awards == 0, "Awards ALBUM not unique on (tmdb_id, release_group_id)."
assert dup_lfm == 0, "Last.fm ALBUM not unique on (tmdb_id, release_group_id)."

album_keys = ["tmdb_id", "release_group_id"]  # or release_group_mbid

albums_merged = merge_layer(albums_awards_df, albums_lfm_df, album_keys, "ALBUM lastfm")

st.markdown("""
Let's do a post\\-merge check
""")

lfm_populated = albums_merged["lfm_album_listeners"].notna().sum()
print("Albums with lfm_album_listeners not-null:", int(lfm_populated),
      f"({round(lfm_populated / len(albums_merged) * 100, 2)}%)")

st.markdown("""
Findings: Last\\.fm album metrics attach cleanly for nearly the full album universe, with listener counts populated for 98\\.6% of albums, indicating strong key alignment and no merge\\-related data loss\\.
""")

st.markdown("""
# IV\\. Artist dataframe merge
""")

st.markdown("""
Next, the artist dataframes
""")

# ------------------------------------------------------------
# 1) Pre-merge uniqueness checks at artist grain
# ------------------------------------------------------------
KEYS = ["artist_id"]  # swap to ["artist_mbid"] if that's your spine key

dup_awards = artists_awards_df.duplicated(subset=KEYS).sum()
dup_lfm    = artists_lfm_df.duplicated(subset=KEYS).sum()

print("Awards duplicate artist keys:", int(dup_awards))
print("Last.fm duplicate artist keys:", int(dup_lfm))

assert dup_awards == 0, "Awards ARTIST not unique on artist key."
assert dup_lfm == 0, "Last.fm ARTIST not unique on artist key."

artist_keys = ["artist_id"]  # or ["artist_mbid"]

artists_merged = merge_layer(artists_awards_df, artists_lfm_df, artist_keys, "ARTIST lastfm")

lfm_populated = artists_merged["lfm_artist_listeners"].notna().sum()
print("Artists with lfm_artist_listeners not-null:", int(lfm_populated),
      f"({round(lfm_populated / len(artists_merged) * 100, 2)}%)")

st.markdown("""
Findings: Artist Last\\.fm coverage is strong: listener counts populated for 97\\.9% of artists \\(2,378\\)\\.
""")

st.markdown("""
# V\\. Tracks dataframe merge
""")

st.markdown("""
Next, the track dataframes
""")

# ------------------------------------------------------------
# 1) Pre-merge uniqueness checks at track grain
# ------------------------------------------------------------
KEYS = ["track_id"]  # swap to ["recording_mbid"] if that's your canonical track key

dup_awards = tracks_awards_df.duplicated(subset=KEYS).sum()
dup_lfm    = tracks_lfm_df.duplicated(subset=KEYS).sum()

print("Awards duplicate track keys:", int(dup_awards))
print("Last.fm duplicate track keys:", int(dup_lfm))

assert dup_awards == 0, "Awards TRACK not unique on track key."
assert dup_lfm == 0, "Last.fm TRACK not unique on track key."

dup_audio = tracks_audio_df.duplicated(subset=KEYS).sum()

print("Audio duplicate track keys:", int(dup_audio))

assert dup_audio == 0, "Audio TRACK not unique on track key."

track_keys = ["track_id"]  # or ["recording_mbid"]

tracks_merged = merge_layer(tracks_awards_df, tracks_lfm_df, track_keys, "TRACK lastfm")

tracks_merged = merge_layer(tracks_merged, tracks_audio_df, track_keys, "TRACK audio")

lfm_populated = tracks_merged["lfm_track_listeners"].notna().sum()
print("Tracks with lfm_track_listeners not-null:", int(lfm_populated),
      f"({round(lfm_populated / len(tracks_merged) * 100, 2)}%)")

st.markdown("""
Findings: Track\\-level coverage is lower \\(as expected\\): listener counts populated for 91\\.7% of tracks \\(72,427\\)
""")

st.markdown("""
# VI\\. Wide dataframe merge
""")

st.markdown("""
Finally, the wide dataframes
""")

# ------------------------------------------------------------
# 1) Pre-merge uniqueness checks at wide grain
# ------------------------------------------------------------
KEYS = ["tmdb_id", "release_group_id", "track_id"]  # match your WIDE grain exactly

dup_awards = wide_awards_df.duplicated(subset=KEYS).sum()
dup_lfm    = wide_lfm_df.duplicated(subset=KEYS).sum()

print("Awards duplicate wide keys:", int(dup_awards))
print("Last.fm duplicate wide keys:", int(dup_lfm))

assert dup_awards == 0, "Awards WIDE not unique on (tmdb_id, release_group_id, track_id)."
assert dup_lfm == 0, "Last.fm WIDE not unique on (tmdb_id, release_group_id, track_id)."

dup_audio = wide_audio_df.duplicated(subset=KEYS).sum()

print("Audio duplicate wide keys:", int(dup_audio))

assert dup_audio == 0, "Audio WIDE not unique on (tmdb_id, release_group_id, track_id)."

wide_keys = ["tmdb_id", "release_group_id", "track_id"]  # match your wide grain

wide_merged = merge_layer(wide_awards_df, wide_lfm_df, wide_keys, "WIDE lastfm")

wide_merged = merge_layer(wide_merged, wide_audio_df, wide_keys, "WIDE audio")

lfm_populated = wide_merged["lfm_track_listeners"].notna().sum()
print("Wide rows with lfm_track_listeners not-null:", int(lfm_populated),
      f"({round(lfm_populated / len(wide_merged) * 100, 2)}%)")

st.markdown("""
Findings: WIDE matches track coverage exactly \\(same 72,427 / 91\\.7%\\), confirming clean propagation into WIDE with no additional loss\\. 
""")

st.markdown("""
# VII\\. Write to pipeline directory
""")

# ------------------------------------------------------------
# Write merged "join everything so far" outputs
# ------------------------------------------------------------
albums_out_path  = "./pipeline/4.5.Albums_join_everything.csv"
artists_out_path = "./pipeline/4.5.Artists_join_everything.csv"
tracks_out_path  = "./pipeline/4.5.Tracks_join_everything.csv"
wide_out_path    = "./pipeline/4.5.Wide_join_everything.csv"

albums_merged.to_csv(albums_out_path, index=False)
artists_merged.to_csv(artists_out_path, index=False)
tracks_merged.to_csv(tracks_out_path, index=False)
wide_merged.to_csv(wide_out_path, index=False)

print("Wrote:", albums_out_path)
print("Wrote:", artists_out_path)
print("Wrote:", tracks_out_path)
print("Wrote:", wide_out_path)
