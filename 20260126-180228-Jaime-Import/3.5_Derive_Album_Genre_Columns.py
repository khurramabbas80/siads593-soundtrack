import streamlit as st
import os, sys

st.set_page_config(page_title="3.5 Derive Album Genre Columns", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# I\\. Setup and Album Title Inspection
""")

# Imports
import pandas as pd
import numpy as np
import os
from datetime import datetime

# os.chdir("/work")  # path adjusted for Streamlit
print(os.listdir("./pipeline"))

# Load the albums dataframe

album_df = pd.read_csv("./pipeline/3.4.Albums_canonical_identified_df.csv")
st.dataframe(album_df.shape)
st.dataframe(album_df.columns)
st.dataframe(album_df.head())

st.markdown("""
Let's look closely at the available tags
""")

null_rg_tags = album_df['rg_tags_text'].isna().sum()
print(f"% of rg tags that are null: {null_rg_tags/len(album_df):.3f}")
  # 80.5% of rg tags are null. So the % of albums with tag genres is quite low, but it's the
  # closest approximation to genre that we have

null_release_tags = album_df['release_tags_text'].isna().sum()
print(f"% of release tags that are null: {null_release_tags/len(album_df):.3f}")
  # 92% of release tags are null. This is an even higher null percentage

null_label_tags = album_df['label_tags_text'].isna().sum()
print(f"% of label tags that are null: {null_label_tags/len(album_df):.3f}")
  # 42% of label tags are null -- so more than half are available. However, I'm not sure how useful
  # label tags would be for our analysis

# Count the number of albums with rg tags but not release tags
release_na = len(album_df[(album_df['rg_tags_text'].notna()) & (album_df['release_tags_text'].isna())])

# Count the number of albums with release tags but not rg tags
rg_na = len(album_df[(album_df['rg_tags_text'].isna()) & (album_df['release_tags_text'].notna())])

# Count the number of albums with values on both
neither_na = len(album_df[(album_df['rg_tags_text'].notna()) & (album_df['release_tags_text'].notna())])

# Count the number where they are both na
both_na = len(album_df[(album_df['rg_tags_text'].isna()) & (album_df['release_tags_text'].isna())])

print(f"Both null: {both_na}: {both_na/len(album_df):.3f}")
print(f"Release Group null / Release has value: {rg_na}: {rg_na/len(album_df):.3f}")
print(f"Release null / Release Group has value: {release_na}: {release_na/len(album_df):.3f}")
print(f"Neither null: {neither_na}: {neither_na/len(album_df):.3f}")


st.markdown("""
Findings: 14% of rg\\_tags\\_text have values where Release has not, and 5\\.5% populate both\\. Therefore, it makes sense to treat rg\\_tags\\_text as the primary source for tags\\. However, about 2% of albums contain release tags but no rg\\_tags, so combining both may provide some marginal value\\. 
""")

album_df['label_tags_text'].value_counts()

st.markdown("""
Findings: A long list of random values with occasional genre information\\. However, leveraging genres at the label\\-level \\(a fundamentally different entity from the album\\) will likely degrade the quality of our genre signal, and potentially introduce a lot of garbage data if we're not careful\\. So, we will not use label tags\\.
""")

st.markdown("""
# II\\. Album Genre exploding
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

album_df[album_df['release_tags_text'].notna()].to_csv('./tmp/release_tags.csv')

CANONICAL_GENRE_MAP = {
    "classical_orchestral": [
        # Core classical buckets
        "classical", "modern classical", "contemporary classical",
        "cinematic classical", "orchestral", "instrumental",
        "baroque", "opera", "chamber music",
        "minimalism", "neoclassicism", "classical crossover",
        # Helpful additions seen in release tags (style, not role)
        "neo-classical", "post-classical",
        "orchestral music", "string quartet"
    ],

    "electronic": [
        # Broad electronic umbrella
        "electronic", "electronica", "electro", "edm",
        "techno", "house", "deep house", "progressive house",
        "trance", "dubstep", "idm", "ebm",
        "electro-industrial", "electroacoustic", "industrial",
        # Additions that show up heavily in soundtrack-style tags
        "synthwave", "retrowave",
        "darkwave", "minimal synth",
        "future bass", "breakcore", "chiptune"
    ],

    "ambient_experimental": [
        # Ambient + experimental umbrella
        "ambient", "dark ambient", "drone", "downtempo",
        "chillout", "lounge", "new age",
        "experimental", "avant-garde", "noise",
        "sound art", "atmospheric", "cinematic",
        # Texture/style additions (common in release tags)
        "darksynth", "soundscape",
        "musique concrete", "sound collage",
        "experimental electronic", "cinematic ambient",
        "atmospheric ambient"
    ],

    "rock": [
        "rock", "alternative rock", "indie rock", "post-rock",
        "hard rock", "classic rock", "punk", "post-punk",
        "grunge", "psychedelic rock", "shoegaze",
        "progressive rock", "krautrock",
        "metal", "heavy metal", "death metal", "black metal",
        "alternative metal", "progressive metal",
        # Minor additions that appear in release tag space
        "instrumental rock", "arena rock"
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
        "bhangra", "celtic", "polka",
        # Useful add from release tags
        "indian classical"
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

# ---- 2. Build a base (rg_tags_text and release_tags_text union)
album_genre_base = album_df.loc[:, ["release_group_mbid", "rg_tags_text", "release_tags_text"]].copy()

def _join_tags(a, b):
    a = "" if pd.isna(a) else str(a).strip()
    b = "" if pd.isna(b) else str(b).strip()
    if a and b:
        return f"{a} | {b}"
    return a or b or None

album_genre_base["tags_text_union"] = album_genre_base.apply(
    lambda r: _join_tags(r["rg_tags_text"], r["release_tags_text"]),
    axis=1
)

st.markdown("""
### II\\.3 Build a genre\\_append table
""")

# ---- 3. Explode the tags -- creates one row per tag
tags_long = album_genre_base.loc[
    album_genre_base["tags_text_union"].notna() & (album_genre_base["tags_text_union"] != ""), :].copy()

tags_long["raw_tag"] = tags_long["tags_text_union"].str.split(" | ", regex=False)
tags_long = tags_long.explode("raw_tag", ignore_index=True)

# Normalize
tags_long["raw_tag"] = tags_long["raw_tag"].str.strip().str.lower()

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

st.markdown("""
### Wide Table
""")

# Load the albums dataframe

wide_df = pd.read_csv("./pipeline/3.4.Wide_canonical_identified_df.csv")
st.dataframe(wide_df.shape)
st.dataframe(wide_df.columns)
st.dataframe(wide_df.head())

GENRE_COLS = list(CANONICAL_GENRE_MAP.keys())

# The column names in genre_append worked fine when merging with the album table
# However, when we merge with the wide table, we really should append album to it
genre_append_renamed = genre_append.rename(
    columns={g: f"album_{g}" for g in GENRE_COLS}
)

wide_before = wide_df.shape

wide_mrg_df = wide_df.merge(
    genre_append_renamed,   # release_group_mbid + album_genre__*
    on="release_group_mbid",
    how="left",
    validate="m:1"
)

print("wide_df shape before:", wide_before)
print("wide_df shape after: ", wide_mrg_df.shape)
st.dataframe(wide_mrg_df.columns)

# Remember that we renamed the columns for wide
WIDE_GENRE_COLS = [f"album_{g}" for g in GENRE_COLS]

# Coverage check
has_any_genre = wide_mrg_df[WIDE_GENRE_COLS].any(axis=1)
print("Wide rows with >=1 genre:", has_any_genre.sum())
print("Pct wide rows with >=1 genre:", has_any_genre.mean())

album_level = wide_mrg_df.groupby("release_group_mbid")[WIDE_GENRE_COLS].max()
print("Release groups with >=1 genre:", album_level.any(axis=1).sum())
print("Pct release groups with >=1 genre:", album_level.any(axis=1).mean())

canon_rows = wide_mrg_df["is_canonical_soundtrack"] == True
song_rows = wide_mrg_df["is_canonical_songtrack"] == True

print("Pct canonical rows with >=1 genre:",
      wide_mrg_df.loc[canon_rows, WIDE_GENRE_COLS].any(axis=1).mean())

print("Pct non-canonical rows with >=1 genre:",
      wide_mrg_df.loc[~canon_rows, WIDE_GENRE_COLS].any(axis=1).mean())

print("Pct song-canonical rows with >=1 genre:",
      wide_mrg_df.loc[song_rows, WIDE_GENRE_COLS].any(axis=1).mean())


# Spot check a few rows where genres exist
display(
    wide_mrg_df.loc[wide_mrg_df[WIDE_GENRE_COLS].any(axis=1),
                 ["release_group_mbid", "album_title", "track_id", "track_title"] + WIDE_GENRE_COLS]
    .sample(20)
)

st.markdown("""
# III\\. Other tag inspection
""")

# Load the albums dataframe

artists_df = pd.read_csv("./pipeline/3.4.Artists_canonical_identified_df.csv")
tracks_df = pd.read_csv("./pipeline/3.4.Tracks_canonical_identified_df.csv")

st.dataframe(artists_df.columns)
st.dataframe(tracks_df.columns)

null_artist_tags = artists_df['artist_tags_text'].isna().sum()
print(f"% of artist tags that are null: {null_artist_tags/len(artists_df):.3f}")
# 60% of tags are null

artists_df['artist_tags_text'].to_csv('./tmp/artist_tags.csv')

st.markdown("""
Findings: Artists tags might be an interesting attribute to explode\\. However, they are far noisier than album or release tags, often conflating genre with role, geography, and miscellaneous metadata, which limits their usefulness as a clean genre signal without heavy preprocessing\\.
""")

null_rec_tags = tracks_df['recording_tags_text'].isna().sum()
print(f"% of recording tags that are null: {null_rec_tags/len(tracks_df):.3f}")
  # 96% of recording tags are null. This column is unusable

st.markdown("""
Findings: Track\\-level tags are pretty useless\\. They are mostly null\\.
""")

st.markdown("""
# IV\\. Exploding Artists tags
""")

st.markdown("""
### IV\\.1 Create a mapping dictionary for artist roles
""")

CANONICAL_ARTIST_ROLE_MAP = {
    "media_composer_orchestral": [
        # Core media composition roles
        "film composer", "television composer", "tv composer",
        "video game composer", "game composer", "vgm",
        "score", "soundtrack", "film score",

        # Classical / orchestral context
        "composer", "orchestral", "orchestra",
        "modern classical", "contemporary classical",
        "cinematic classical", "classical composer"
    ],

    "pop_vocalist": [
        "pop", "pop rock", "dance-pop", "electropop",
        "singer", "vocalist", "female vocals", "male vocalist",
        "adult contemporary", "teen pop", "pop soul"
    ],

    "rock_artist_band": [
        "rock", "alternative rock", "indie rock",
        "classic rock", "hard rock",
        "punk", "post-punk", "grunge",
        "psychedelic rock", "shoegaze",
        "band", "guitarist"
    ],

    "electronic_producer": [
        "electronic", "electronica", "edm",
        "techno", "house", "deep house", "progressive house",
        "trance", "dubstep", "idm",
        "synth", "synthwave", "retrowave",
        "producer", "dj"
    ],

    "jazz_traditional": [
        "jazz", "blues", "soul jazz", "hard bop",
        "bebop", "post-bop", "big band",
        "swing", "vocal jazz", "instrumental jazz"
    ],

    "world_folk_regional": [
        "world", "folk", "traditional",
        "americana", "country", "bluegrass",
        "latin", "latin pop",
        "afrobeat", "afrobeats",
        "bhangra", "celtic"
    ]
}

st.markdown("""
### IV\\.2 Append and merge with artists\\_df
""")

st.markdown("""
We'll translate and merge this in a similar way to the album tags\\.
""")

# ---- 1. Build reverse lookup: raw_tag -> canonical artist role cluster ----
# (If a raw tag appears in multiple canons, we keep the first; you can change this later.)
artist_tag_to_canon = {}
for canon, syns in CANONICAL_ARTIST_ROLE_MAP.items():
    for s in syns:
        key = str(s).strip().lower()
        if key and key not in artist_tag_to_canon:
            artist_tag_to_canon[key] = canon

ARTIST_ROLE_COLS = list(CANONICAL_ARTIST_ROLE_MAP.keys())

# ---- 2. Build a base (artist_tags_text and artist_tag_text union, if you have both) ----
# Assumes artist_df has at least: artist_id, artist_tags_text
# If you only have one tags column, this still works (it just passes through).
artist_role_base = artists_df.loc[:, ["artist_id", "artist_tags_text"]].copy()

# ---- 3. Explode the tags -- creates one row per tag ----
tags_long_artist = artist_role_base.loc[
    artist_role_base["artist_tags_text"].notna() & (artist_role_base["artist_tags_text"] != ""),
    :
].copy()

tags_long_artist["raw_tag"] = tags_long_artist["artist_tags_text"].str.split(" | ", regex=False)
tags_long_artist = tags_long_artist.explode("raw_tag", ignore_index=True)

# Normalize tags
tags_long_artist["raw_tag"] = tags_long_artist["raw_tag"].astype(str).str.strip().str.lower()

st.dataframe(tags_long_artist.head())

# ---- 4. Map raw tags to canonical artist role cluster ----
tags_long_artist["canonical_artist_role"] = tags_long_artist["raw_tag"].map(artist_tag_to_canon)
tags_mapped_artist = tags_long_artist.dropna(subset=["canonical_artist_role"]).copy()

st.dataframe(tags_mapped_artist.head())

# ---- 5. Build the artist_role_append table at artist grain (boolean flag per canonical cluster) ----
artist_role_append = tags_mapped_artist.assign(present=1).pivot_table(
    index="artist_id",
    columns="canonical_artist_role",
    values="present",
    aggfunc="max",
    fill_value=0
).reset_index()

# Ensure all expected columns exist, even if a cluster didn't appear in the data
for c in ARTIST_ROLE_COLS:
    if c not in artist_role_append.columns:
        artist_role_append[c] = 0
    artist_role_append[c] = artist_role_append[c].astype(bool)

st.dataframe(artist_role_append.head())

# ---- 6. Merge back to artist_df (optional) ----
artists_df_w_roles = artists_df.merge(
    artist_role_append,
    on="artist_id",
    how="left",
    validate="m:1"
)

# Fill missing role flags with False (artists with no tags or no mapped tags)
for c in ARTIST_ROLE_COLS:
    artists_df_w_roles[c] = artists_df_w_roles[c].fillna(False).astype(bool)

st.dataframe(artists_df_w_roles.head())

st.markdown("""
### IV\\.3 Append to wide table
""")

# --------------------------------------------------------------------------
# BIG PICTURE: Attach Artist-role flags (artist-grained) to the track-grained
# wide table WITHOUT exploding rows.
#
# Strategy mirrors the Last.fm bridge:
#   0) Precompute primary artist fields on wide_mrg_df (e.g., primary_artist_id),
#      so we don't repeat string parsing in every enrichment step.
#   1) Collapse to one row per album (release_group_mbid), keeping primary_artist_id.
#   2) Join primary_artist_id to artist_role_append (one row per artist).
#   3) Prefix role flags with 'Artist_' so they stay distinct from album_ genres.
#   4) Merge album-level artist role flags back to wide_mrg_df on release_group_mbid.
# --------------------------------------------------------------------------

# STEP 0: Pull a single “primary” album artist onto every track row
# Why:
# - album_artist_* fields are stored as pipe-delimited lists ("id1 | id2 | ...")
# - we keep the *first* artist as the default “album artist” for album-level enrichments
# - doing this once up front keeps later joins clean and avoids repeating string parsing

wide_mrg_df = wide_mrg_df.copy()  # don’t mutate the original wide table in-place

# --- primary_artist_id ---
# Split on pipes with optional whitespace, then take the first token (index 0).
# Use to_numeric(errors="coerce") so blanks / weird tokens become <NA> instead of crashing.
primary_artist_id_raw = (
    wide_mrg_df["album_artist_ids_text"]
      .astype("string")
      .str.strip()
      .str.split(r"\s*\|\s*", regex=True)
      .str[0]
)

# Nullable integer dtype ("Int64") plays nicely with missing values.
wide_mrg_df["primary_artist_id"] = pd.to_numeric(
    primary_artist_id_raw, errors="coerce"
).astype("Int64")

# --- primary_artist (name) ---
# Same logic for the human-readable name list (handy for spot checks + QA).
wide_mrg_df["primary_artist"] = (
    wide_mrg_df["album_artist_names_text"]
      .astype("string")
      .str.strip()
      .str.split(r"\s*\|\s*", regex=True)
      .str[0]
)

# --- Quick QA ---
# 1) Coverage: do we actually have primary artist values most of the time?
print("primary_artist_id missing:", wide_mrg_df["primary_artist_id"].isna().mean())
print("primary_artist missing:", wide_mrg_df["primary_artist"].isna().mean())

# 2) Consistency: within a release_group (album), primary_artist_id should not vary across tracks.
# If this is >0, it usually means the wide table has inconsistent artist lists across track rows.
inconsistent = (
    wide_mrg_df.groupby("release_group_mbid")["primary_artist_id"]
    .nunique(dropna=True)
)
print("albums with >1 primary_artist_id:", (inconsistent > 1).sum())

ARTIST_ROLE_COLS = list(CANONICAL_ARTIST_ROLE_MAP.keys())

# 1) One row per album: release_group_mbid → primary_artist_id
#    (primary_artist_id is already computed in Step 0)
album_primary_artist = (
    wide_mrg_df[["release_group_mbid", "primary_artist_id"]]
    .drop_duplicates(subset=["release_group_mbid"])
)


# 2) Prefix artist role flags (keep artist_id as the join key for now)
artist_role_append_renamed = artist_role_append.rename(
    columns={c: f"Artist_{c}" for c in ARTIST_ROLE_COLS}
)

# 3) Join role flags onto primary_artist_id (artist-grain join)
album_primary_artist_roles = album_primary_artist.merge(
    artist_role_append_renamed.rename(columns={"artist_id": "primary_artist_id"}),
    on="primary_artist_id",
    how="left",
    validate="m:1"
)

# 4) Keep only the columns we want to append back to wide (album grain)
cols_to_add = ["release_group_mbid", "primary_artist_id"] + [f"Artist_{c}" for c in ARTIST_ROLE_COLS]
album_primary_artist_roles = album_primary_artist_roles[cols_to_add].rename(
    columns={"primary_artist_id": "album_primary_artist_id"}
)

# Optional: fill missing role flags as False (albums whose primary artist had no tags / no mappings)
for c in [f"Artist_{x}" for x in ARTIST_ROLE_COLS]:
    if c in album_primary_artist_roles.columns:
        album_primary_artist_roles[c] = album_primary_artist_roles[c].fillna(False).astype(bool)

# 5) Merge back into the track-grained wide table
before = wide_mrg_df.shape

wide_mrg_df2 = wide_mrg_df.merge(
    album_primary_artist_roles,
    on="release_group_mbid",
    how="left",
    validate="m:1"
)

print("wide_mrg_df shape before:", before)
print("wide_mrg_df shape after: ", wide_mrg_df2.shape)
st.dataframe(wide_mrg_df2.columns)

st.markdown("""
# V\\. Write everything back out
""")

out_path = "./pipeline/3.5.Albums_exploded_genre.csv"

album_merged_df.to_csv(out_path, index=False)

out_path = "./pipeline/3.5.Artists_exploded_genre.csv"

artists_df_w_roles.to_csv(out_path, index=False)

st.markdown("""
Tracks just carry over
""")

import shutil

shutil.copy(
    "./pipeline/3.4.Tracks_canonical_identified_df.csv",
    "./pipeline/3.5.Tracks_exploded_genre.csv"
)


out_path = "./pipeline/3.5.Wide_exploded_genre.csv"

wide_mrg_df2.to_csv(out_path, index=False)
