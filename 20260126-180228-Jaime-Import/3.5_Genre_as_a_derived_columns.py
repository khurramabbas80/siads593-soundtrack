import streamlit as st
import os, sys

st.set_page_config(page_title="3.5 Genre as a derived columns", layout="wide")

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

# Imports
import pandas as pd
import numpy as np
import os
from datetime import datetime

print(os.listdir("./pipeline"))

# Load the albums dataframe

album_df = pd.read_csv("./pipeline/3.4.Albums_canonical_identified_df.csv")
st.dataframe(album_df.shape)
st.dataframe(album_df.columns)
st.dataframe(album_df.head())

st.markdown("""
# II\\. Genre exploding
""")

st.markdown("""
As mentioned in an early notebook, we want to simplify genre into seven buckets, and append these as features to the album\\_df
""")

st.markdown("""
Recap: We want to take the free\\-form rg\\_tags\\_text field, split it into individual tags, and map any tags that matches our curated synonym list into a small set of canonical genres \\(7 max\\)\\. Then we roll that back up to the album grain \\(release\\_group\\_mbid\\) to produce a compact genre\\_append table with boolean flags \\(e\\.g\\., electronic=True, rock=False, etc\\.\\) that can be merged into both album\\_df and wide\\_df\\.
""")

st.markdown("""
### II\\.1 Genre thesaurus
""")

CANONICAL_GENRE_MAP = {
    "classical_orchestral": [
        "classical", "modern classical", "contemporary classical",
        "cinematic classical", "orchestral", "instrumental",
        "baroque", "opera", "chamber music",
        "minimalism", "neoclassicism", "classical crossover"
    ],
    "electronic": [
        "electronic", "electronica", "electro", "edm",
        "techno", "house", "deep house", "progressive house",
        "trance", "dubstep", "idm", "ebm",
        "electro-industrial", "electroacoustic", "industrial"
    ],
    "ambient_experimental": [
        "ambient", "dark ambient", "drone", "downtempo",
        "chillout", "lounge", "new age",
        "experimental", "avant-garde", "noise",
        "sound art", "atmospheric", "cinematic"
    ],
    "rock": [
        "rock", "alternative rock", "indie rock", "post-rock",
        "hard rock", "classic rock", "punk", "post-punk",
        "grunge", "psychedelic rock", "shoegaze",
        "progressive rock", "krautrock",
        "metal", "heavy metal", "death metal", "black metal",
        "alternative metal", "progressive metal"
    ],
    "pop": [
        "pop", "electropop", "synth-pop", "dance-pop",
        "indie pop", "art pop", "chamber pop",
        "europop", "traditional pop", "pop rock"
    ],
    "hip_hop_rnb": [
        "hip hop", "hip-hop", "hiphop", "rap",
        "trap", "grime", "drill", "gangsta rap", "pop rap",
        "r&b", "rhythm and blues", "contemporary r&b",
        "neo soul", "soul", "funk", "disco"
    ],
    "world_folk": [
        "world", "folk", "contemporary folk", "alternative folk",
        "americana", "country", "outlaw country", "bluegrass",
        "latin", "latin pop", "reggae", "ska",
        "afrobeat", "afrobeats", "afroswing",
        "bhangra", "celtic", "polka"
    ]
}

st.markdown("""
### II\\.2 Reverse lookup dictionary
""")

# ---- 1. Build reverse lookup: raw_tag -> canonical genre ----
# (If a raw tag appears in multiple canons, we keep the first; you can change this later.)
tag_to_canon = {}
for canon, syns in CANONICAL_GENRE_MAP.items():
    for s in syns:
        key = str(s).strip().lower()
        if key and key not in tag_to_canon:
            tag_to_canon[key] = canon

GENRE_COLS = list(CANONICAL_GENRE_MAP.keys())

st.write(tag_to_canon)
# Reverse lookup created:
# {'classical': 'classical_orchestral',
# 'modern classical': 'classical_orchestral',
# 'contemporary classical': 'classical_orchestral', ..

# ---- 2. Build a base
album_genre_base = album_df.loc[:, ['release_group_mbid', 'rg_tags_text']].copy()

st.markdown("""
### II\\.3 Build a genre\\_append table
""")

# ---- 3. Explode the tags -- creates one row per tag
tags_long = album_genre_base.loc[album_genre_base['rg_tags_text'] != "", :].copy()
tags_long['raw_tag'] = tags_long['rg_tags_text'].str.split(" | ", regex = False)
tags_long = tags_long.explode("raw_tag", ignore_index=True)

st.dataframe(tags_long.head())

# ---- 4. Map raw tags to canonical genre
tags_long['canonical_genre'] = tags_long['raw_tag'].map(tag_to_canon)  # This will result in lots of empty mappings for junk tags
tags_mapped = tags_long.dropna(subset = ['canonical_genre']).copy()

# Confirm mapping
st.dataframe(tags_mapped.head())

# ---- 5. Build the genre_append table at album grain (boolean flag per canonical genre)
genre_append = tags_mapped.assign(present = 1).pivot_table(
    index = 'release_group_mbid',
    columns = 'canonical_genre',
    values = 'present',
    aggfunc = 'max',
    fill_value = 0
).reset_index()

# Convert to boolean flags
for g in GENRE_COLS:
    genre_append[g] = genre_append[g].astype(bool)

st.write(genre_append)

st.markdown("""
### II\\.4 Append the genre table
""")

st.markdown("""
### Album
""")

# Merge genre flags onto album_df (album grain: release_group_mbid)
# Left join so we don't drop any albums that don't have genre tags.
album_merged_df = album_df.merge(
    genre_append,
    on="release_group_mbid",
    how="left"
)

print("album_df rows:", len(album_df),"album_merged_df rows:", len(album_merged_df))
print("Unique release_group_mbids in album_merged_df:", album_merged_df["release_group_mbid"].nunique())

# 3) Quick inspection
print("\nGenre columns added:", len(GENRE_COLS))
print("Albums with ANY genre flagged:", (album_merged_df[GENRE_COLS].any(axis=1)).sum())
print("Albums with NO genre flagged:", (~album_merged_df[GENRE_COLS].any(axis=1)).sum())
print("% albums with ANY genre flagged:", (album_merged_df[GENRE_COLS].any(axis=1)).sum()/len(album_merged_df))

# How many genres per album (helps detect overly-broad tagging)
genre_ct = album_merged_df[GENRE_COLS].sum(axis=1)
print("\nGenres per album (distribution):")
st.write(genre_ct.value_counts().sort_index().head(25))

# Top genres overall (coverage)
top_genres = album_merged_df[GENRE_COLS].sum().sort_values(ascending=False)
print("\nTop genres (albums flagged):")
st.write(top_genres.head(25))

# Spot check a few rows where genres exist
display(
    album_merged_df.loc[album_merged_df[GENRE_COLS].any(axis=1),
                 ["release_group_mbid", "album_title"] + GENRE_COLS]
    .head(20)
)

out_path = "./pipeline/3.5.Albums_exploded_genre.csv"

album_merged_df.to_csv(out_path, index=False)

st.markdown("""
### Wide Table
""")

# Load the albums dataframe

wide_df = pd.read_csv("./pipeline/3.4.Wide_canonical_identified_df.csv")
st.dataframe(wide_df.shape)
st.dataframe(wide_df.columns)
st.dataframe(wide_df.head())

wide_before = len(wide_df)

wide_mrg_df = wide_df.merge(
    genre_append,   # includes release_group_mbid + GENRE_COLS
    on="release_group_mbid",
    how="left",
    validate="m:1"
)

print("wide_df rows before:", wide_before)
print("wide_df rows after: ", len(wide_df))

# Coverage check
has_any_genre = wide_mrg_df[GENRE_COLS].any(axis=1)
print("Wide rows with >=1 genre:", has_any_genre.sum())
print("Pct wide rows with >=1 genre:", has_any_genre.mean())

album_level = wide_mrg_df.groupby("release_group_mbid")[GENRE_COLS].max()
print("Release groups with >=1 genre:", album_level.any(axis=1).sum())
print("Pct release groups with >=1 genre:", album_level.any(axis=1).mean())

canon_rows = wide_mrg_df["is_canonical_soundtrack"] == True
song_rows = wide_mrg_df["is_canonical_songtrack"] == True

print("Pct canonical rows with >=1 genre:",
      wide_mrg_df.loc[canon_rows, GENRE_COLS].any(axis=1).mean())

print("Pct non-canonical rows with >=1 genre:",
      wide_mrg_df.loc[~canon_rows, GENRE_COLS].any(axis=1).mean())

print("Pct song-canonical rows with >=1 genre:",
      wide_mrg_df.loc[song_rows, GENRE_COLS].any(axis=1).mean())


# Spot check a few rows where genres exist
display(
    wide_mrg_df.loc[wide_mrg_df[GENRE_COLS].any(axis=1),
                 ["release_group_mbid", "album_title", "track_id", "track_title"] + GENRE_COLS]
    .sample(20)
)

out_path = "./pipeline/3.5.Wide_exploded_genre.csv"

wide_mrg_df.to_csv(out_path, index=False)
