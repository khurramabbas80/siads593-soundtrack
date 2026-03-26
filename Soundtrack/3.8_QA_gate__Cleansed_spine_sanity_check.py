import streamlit as st
import os, sys

st.set_page_config(page_title="3.8 QA gate_ Cleansed spine sanity check", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# I\\. Overview
""")

st.markdown("""
Notebook purpose & scope — 3\\.8 Cleansed spine QA gate
""")

st.markdown("""
This notebook implements the 3\\.8 QA gate: Cleansed spine sanity check\\.
Its purpose is to validate the cumulative outcome of the spine cleansing and flagging steps \\(3\\.2–3\\.6\\), not to re\\-test individual rules or heuristics that have already been unit\\-checked upstream\\.
""")

st.markdown("""
At this point in the pipeline, some rows have already been removed for data integrity \\(e\\.g\\., unofficial releases, ambiguous album–film mappings\\), while other constraints \\(canonical soundtrack selection, vote\\_count \\> 500\\) have been applied as flags only, not enforced reductions\\. This notebook exists to make the impact of both explicit before moving into enrichment and metric analysis\\.
""")

st.markdown("""
This is a system\\-level integration check, not exploratory analysis\\.
""")

st.markdown("""
What this notebook should cover
""")

st.markdown("""
1\\. Integrity impact of cleansing steps \\(3\\.2–3\\.3\\)
\\- Quantify before/after row counts at the album level to understand the impact of cleansing\\.
\\- Summarize high\\-level categories of dropped rows \\(e\\.g\\., unofficial releases, duplicate or conflicting album–film matches\\)\\.
\\- Confirm that overall coverage remains healthy after cleansing \\(films with ≥1 album, albums per film, tracks per album\\)\\.
""")

st.markdown("""
2\\. Spine invariants and uniqueness guarantees
\\- Confirm that no album is linked to more than one film after cleansing\\.
\\- Confirm there are no duplicate \\(tmdb\\_id, release\\_group\\_id\\) pairs in the spine\\.
\\- Confirm that spine keys are stable and uniquely identify rows post\\-cleansing\\.
""")

st.markdown("""
3\\. Canonical soundtrack flag behavior \\(3\\.4\\)
\\- Confirm that each film has exactly one canonical soundtrack\\.
\\- Confirm that no films have zero or multiple canonical soundtracks\\.
\\- Review the distribution of candidate albums per film relative to the canonical selection to ensure coherent behavior\\.
""")

st.markdown("""
4\\. Genre & Composer sanity \\(3\\.5–3\\.7\\)
\\- Confirm that genre explosion did not unintentionally distort the spine\\.
\\- Confirm that composer\\_primary\\_clean is populated for TMDB rows where film\\_soundtrack\\_composer\\_raw = 'Unknown', except when MusicBrainz only provides 'Various Artists'\\.
\\- Confirm that MusicBrainz\\-based composer recovery is deterministic, using the position = 0 credited artist per tmdb\\_id \\(excluding 'Various Artists'\\), with no duplication\\-induced drift\\.
""")

st.markdown("""
5\\. Scoping impact preview via flags \\(3\\.6\\)
\\- Confirm coverage of vote\\_count \\> 500 films by year and in aggregate\\.
\\- Confirm overlap between vote\\_count \\> 500 films and canonical soundtrack coverage\\.
\\- Preview the size and basic characteristics of the universe that would remain if scoping were enforced\\.
""")

st.markdown("""
6\\. Targeted human sanity checks \\(illustrative only\\)
Spot\\-check a small number of high\\-signal, real\\-world entities \\(e\\.g\\., other well\\-known composers or films in addition to the ones we checked in 3\\.6\\) to confirm that nothing obviously implausible survived cleansing\\. These checks are confidence\\-building, not exhaustive\\.
""")

st.markdown("""
# I\\. Impact of the Cleaning
""")

st.markdown("""
This section quantifies how the cumulative cleaning steps affected the size and coverage of the spine\\. It focuses on what changed at each grain \\(film, album, track\\) rather than re\\-evaluating individual cleanup rules\\.
""")

import pandas as pd
import re
import unicodedata

original_album_df = pd.read_csv("./pipeline/2.1.MUSICBRAINZ_mv_tmdb_soundtrack_album_spine_2015_2025.csv")
latest_album_df = pd.read_csv("./pipeline/3.7.Albums_composer_analysis.csv")

original_artist_df = pd.read_csv("./pipeline/2.2.MUSICBRAINZ_mv_tmdb_soundtrack_artist_spine_2015_2025.csv")
latest_artist_df = pd.read_csv("./pipeline/3.7.Artists_composer_analysis.csv")

original_wide_df = pd.read_csv("./pipeline/2.2.MUSICBRAINZ_mv_tmdb_soundtrack_wide_track_2015_2025.csv")
latest_wide_df = pd.read_csv("./pipeline/3.7.Wide_composer_analysis.csv")

st.markdown("""
### I\\.1 Basic shape \\+ key sanity \\(ALBUM / ARTIST / WIDE\\)
""")

st.markdown("""
Question: Did the cleaned pipeline outputs load correctly and look structurally sane \\(row counts, key non\\-nullness\\) across ALBUM, ARTIST, and WIDE?
""")

# Basic shape + key sanity (albums, artists, wide)
# -----------------------------------------------

# ----------------
# ALBUM SPINE
# ----------------
print("=== ALBUM SPINE ===")
print("Original album rows:", len(original_album_df))
print("Latest album rows:  ", len(latest_album_df))

print("Original unique albums:", original_album_df["release_group_mbid"].nunique())
print("Latest unique albums:  ", latest_album_df["release_group_mbid"].nunique())

print("Original unique films:", original_album_df["tmdb_id"].nunique())
print("Latest unique films:  ", latest_album_df["tmdb_id"].nunique())


# ----------------
# ARTIST SPINE
# ----------------
print("\n=== ARTIST SPINE ===")
print("Original artist rows:", len(original_artist_df))
print("Latest artist rows:  ", len(latest_artist_df))

# Artists should have a stable key (often artist_mbid; fallback to artist_id if needed)
artist_key = "artist_mbid" if "artist_mbid" in original_artist_df.columns else "artist_id"

print(f"Original unique artists ({artist_key}):", original_artist_df[artist_key].nunique())
print(f"Latest unique artists ({artist_key}):  ", latest_artist_df[artist_key].nunique())

# Still helpful to track film coverage in the artist spine if tmdb_id is present
if "tmdb_id" in original_artist_df.columns and "tmdb_id" in latest_artist_df.columns:
    print("Original unique films (artist spine):", original_artist_df["tmdb_id"].nunique())
    print("Latest unique films (artist spine):  ", latest_artist_df["tmdb_id"].nunique())
else:
    print("Note: tmdb_id not present in artist spine CSV (skipping film coverage check).")


# ----------------
# WIDE TRACK TABLE
# ----------------
print("\n=== WIDE TRACK TABLE ===")
print("Original wide rows:", len(original_wide_df))
print("Latest wide rows:  ", len(latest_wide_df))

# Wide table should include track_id; if not, fall back to recording_mbid or a composite.
if "track_id" in original_wide_df.columns:
    track_key = "track_id"
elif "recording_mbid" in original_wide_df.columns:
    track_key = "recording_mbid"
else:
    track_key = None

if track_key is not None:
    print(f"Original unique tracks ({track_key}):", original_wide_df[track_key].nunique())
    print(f"Latest unique tracks ({track_key}):  ", latest_wide_df[track_key].nunique())
else:
    print("Note: no obvious track key found (track_id / recording_mbid missing).")

# Wide table should always have film + album coverage
print("Original unique films (wide):", original_wide_df["tmdb_id"].nunique())
print("Latest unique films (wide):  ", latest_wide_df["tmdb_id"].nunique())

print("Original unique albums (wide):", original_wide_df["release_group_mbid"].nunique())
print("Latest unique albums (wide):  ", latest_wide_df["release_group_mbid"].nunique())

st.markdown("""
Findings: At a high level, the cleansing steps had a measured, targeted impact on the spine rather than a broad contraction\\. On the album spine, rows dropped from 5,209 to 4,771, with unique albums aligning one\\-to\\-one with rows post\\-cleaning and film coverage declining from 4,776 to 4,448 — consistent with removing ambiguous or low\\-signal mappings rather than eroding the core universe\\. 
""")

st.markdown("""
The artist spine is unchanged end\\-to\\-end, confirming that the cleansing logic did not unintentionally affect artist attribution or membership\\. 
""")

st.markdown("""
In the wide track table, row count and unique tracks declined modestly \\(85,842 → 78,992\\), with corresponding reductions in film and album coverage that mirror the album\\-level pruning, indicating that downstream track\\-level effects are a direct consequence of album/film decisions rather than independent data loss\\. 
""")

st.markdown("""
Overall, the shape of the data remains intact, with reductions behaving as expected and no signs of accidental cascade loss across layers\\.
""")

st.markdown("""
### I\\.2 Albums dropped vs retained
""")

st.markdown("""
Question: What did we remove during cleansing \\(and how much\\), and are the removals explainable \\(e\\.g\\., unofficial releases, conflict resolution\\)?
""")

# ----------------
# ALBUMS dropped vs retained
# ----------------
orig_albums = set(original_album_df["release_group_mbid"])
latest_albums = set(latest_album_df["release_group_mbid"])

dropped_albums = orig_albums - latest_albums
retained_albums = orig_albums & latest_albums

print("Albums dropped:", len(dropped_albums))
print("Albums retained:", len(retained_albums))
print("Pct albums dropped:", len(dropped_albums) / len(orig_albums))


# ----------------
# ARTISTS dropped vs retained
# ----------------
artist_key = "artist_mbid" if "artist_mbid" in original_artist_df.columns else "artist_id"

orig_artists = set(original_artist_df[artist_key])
latest_artists = set(latest_artist_df[artist_key])

dropped_artists = orig_artists - latest_artists
retained_artists = orig_artists & latest_artists

print("\nArtists dropped:", len(dropped_artists))
print("Artists retained:", len(retained_artists))
print("Pct artists dropped:", len(dropped_artists) / len(orig_artists))


# ----------------
# WIDE TRACKS dropped vs retained
# ----------------
if "track_id" in original_wide_df.columns:
    track_key = "track_id"
elif "recording_mbid" in original_wide_df.columns:
    track_key = "recording_mbid"
else:
    track_key = None

if track_key is not None:
    orig_tracks = set(original_wide_df[track_key])
    latest_tracks = set(latest_wide_df[track_key])

    dropped_tracks = orig_tracks - latest_tracks
    retained_tracks = orig_tracks & latest_tracks

    print(f"\nTracks dropped ({track_key}):", len(dropped_tracks))
    print(f"Tracks retained ({track_key}):", len(retained_tracks))
    print("Pct tracks dropped:", len(dropped_tracks) / len(orig_tracks))
else:
    print("\nNote: no obvious track key found (track_id / recording_mbid missing), skipping tracks dropped/retained.")

st.markdown("""
Finding: The dropped\\-vs\\-retained analysis reinforces that the cleansing was surgical rather than disruptive\\. Only ~3\\.2% of albums were removed \\(157 total\\), with the remaining 4,771 albums carrying forward into the cleaned spine\\. No artists were lost, confirming that pruning operated strictly at the album/track level and did not affect artist coverage\\. At the track level, the impact closely mirrors the album drop rate \\(~3\\.15% of tracks removed\\), indicating that track reductions are a downstream consequence of album\\-level decisions rather than independent over\\-pruning\\. Overall, retention remains high across all entities, with loss patterns tightly aligned and expected\\.
""")

st.markdown("""
### I\\.3 Coverage remains healthy
""")

st.markdown("""
Question: After cleansing, do we still have strong coverage \\(films with ≥1 album, albums per film, tracks per album\\) consistent with expectations?
""")

orig_films = set(original_album_df["tmdb_id"])
latest_films = set(latest_album_df["tmdb_id"])

lost_films = orig_films - latest_films

print("Films completely lost:", len(lost_films))
print("Pct films lost:", len(lost_films) / len(orig_films))

def albums_per_film(df):
    return (
        df.groupby("tmdb_id")["release_group_mbid"]
        .nunique()
        .describe()
    )

print("Albums per film — ORIGINAL")
st.dataframe(albums_per_film(original_album_df))

print("Albums per film — CLEANED")
st.dataframe(albums_per_film(latest_album_df))


films_with_album_orig = original_album_df["tmdb_id"].nunique()
films_with_album_latest = latest_album_df["tmdb_id"].nunique()

print("Films w/ ≥1 album (original):", films_with_album_orig)
print("Films w/ ≥1 album (cleaned): ", films_with_album_latest)

st.markdown("""
Findings: Analyzing the last three QA checks, the cleansing removed 328 films \\(~6\\.9%\\) from the album spine, reducing film coverage from 4,776 to 4,448 films with at least one album\\. Importantly, the albums\\-per\\-film distribution remains stable pre\\- and post\\-cleaning, indicating that entire low\\-signal films were removed rather than thinning legitimate soundtrack coverage within retained films\\.
""")

st.markdown("""
# II\\. Spine integrity and uniqueness checks
""")

st.markdown("""
This section validates that the cleaned spine preserves core structural invariants\\. The checks here confirm that relationships, keys, and row identities are internally consistent and safe to use downstream\\.
""")

st.markdown("""
### II\\.1 Relationship integrity
""")

st.markdown("""
An invariant is a condition that is expected to always hold true once a system reaches a certain stage — regardless of how the data changes elsewhere\\.
""")

st.markdown("""
In this context, an invariant is a non\\-negotiable property of the cleaned spine\\. If it’s violated, something is wrong with the pipeline, not just “interesting about the data\\.”
""")

st.markdown("""
Question: Do the core relationships still line up after cleansing \\(e\\.g\\., WIDE rows map cleanly into the ALBUM spine universe\\)?
""")

# ============================================================
# II. Uniqueness checks (Album + Artist + Wide) — NO DUPLICATE CHECKS
# ============================================================

def run_uniqueness_checks(df, label, film_key, entity_key, entity_label, extra_cols=None):
    """
    Generic invariant checks for a spine-like table keyed by (film_key, entity_key).

    Invariants covered in THIS cell:
      0) Basic key hygiene (required columns present; null counts)
      1) No entity linked to multiple films  (entity_key -> exactly 1 film_key)

    Notes:
      - Duplicate checks are intentionally handled in the next QA cell.
      - Spine-key uniqueness is also handled later (to avoid repetition).
    """
    extra_cols = extra_cols or []

    print(f"\n=== {label} ===")

    # -------------------------------------------------------------------
    # 0) Basic key hygiene
    # -------------------------------------------------------------------
    if film_key not in df.columns or entity_key not in df.columns:
        raise KeyError(f"{label}: Missing required columns: {film_key=} {entity_key=}")

    print("Rows:", len(df))
    print(f"Null {film_key}:", int(df[film_key].isna().sum()))
    print(f"Null {entity_key}:", int(df[entity_key].isna().sum()))

    # If either key is null, invariants can’t hold reliably.
    df_keys = df.dropna(subset=[film_key, entity_key]).copy()

    # -------------------------------------------------------------------
    # 1) Invariant: No entity linked to multiple films
    # -------------------------------------------------------------------
    entity_to_film_nunique = df_keys.groupby(entity_key)[film_key].nunique()
    entities_multi_film = entity_to_film_nunique[entity_to_film_nunique > 1]

    print(f"\n[Invariant] No {entity_label} linked to multiple films")
    print(f"{entity_label.title()} linked to >1 film:", int(len(entities_multi_film)))

    if len(entities_multi_film) > 0:
        offenders = entities_multi_film.sort_values(ascending=False).head(20).index
        cols_to_show = [entity_key, film_key] + [c for c in extra_cols if c in df_keys.columns]
        display(
            df_keys[df_keys[entity_key].isin(offenders)][cols_to_show]
            .drop_duplicates()
            .sort_values([entity_key, film_key])
        )

    # Optional hard assert
    assert len(entities_multi_film) == 0, f"Invariant failed: some {entity_label} map to multiple films."


# ============================================================
# ALBUM SPINE
# ============================================================
run_uniqueness_checks(
    df=latest_album_df.copy(),
    label="ALBUM SPINE (latest_album_df)",
    film_key="tmdb_id",
    entity_key="release_group_mbid",   # change to release_group_id if you use that
    entity_label="album",
    extra_cols=["film_title", "album_title"]
)

# ============================================================
# ARTIST SPINE
# NOTE: No tmdb_id in your CSV, so we do key hygiene only here.
# Duplicate checks (one row per artist) happen in the next QA cell.
# ============================================================
print("\n=== ARTIST SPINE (latest_artist_df) ===")
artist_df = latest_artist_df.copy()
artist_key = "artist_mbid" if "artist_mbid" in artist_df.columns else "artist_id"

print("Rows:", len(artist_df))
print(f"Null {artist_key}:", int(artist_df[artist_key].isna().sum()))

# Optional hard assert
assert artist_df[artist_key].isna().sum() == 0, "Invariant failed: artist key contains nulls."


# ============================================================
# WIDE TRACK TABLE
# ============================================================
run_uniqueness_checks(
    df=latest_wide_df.copy(),
    label="WIDE TRACK TABLE (latest_wide_df)",
    film_key="tmdb_id",
    entity_key="track_id",             # if missing, swap to recording_mbid
    entity_label="track",
    extra_cols=["film_title", "album_title", "track_title"]
)

st.markdown("""
Findings: All core spine invariants hold cleanly across the album, artist, and wide\\-track layers\\. In the album spine, all film and album keys are present and no album is linked to more than one film, confirming a stable one\\-to\\-one album–film relationship after cleansing\\. The artist spine shows complete key coverage with no null artist identifiers, indicating that artist attribution remained intact through the cleaning steps\\. In the wide track table, all film and track keys are present and no track maps to multiple films, demonstrating that track\\-level film association remains structurally sound downstream\\.
""")

st.markdown("""
### II\\.2 No pair\\-level duplication
""")

st.markdown("""
Question: Are there any duplicate key pairs that would violate the intended grain \\(e\\.g\\., duplicate film–album pairs, duplicate film–track pairs\\)?
""")


# ============================================================
# 2) Invariant: No duplicate key pairs
#   - Album spine: (tmdb_id, release_group_mbid)
#   - Wide table:  (tmdb_id, track_id)
#   - Artist spine: one row per artist (artist_mbid)  [no tmdb_id in your artist CSV]
# ============================================================

# ----------------
# ALBUM SPINE
# ----------------
df = latest_album_df.copy()
FILM_KEY = "tmdb_id"
ALBUM_KEY = "release_group_mbid"

df_keys = df.dropna(subset=[FILM_KEY, ALBUM_KEY]).copy()

dup_pairs = df_keys.duplicated(subset=[FILM_KEY, ALBUM_KEY], keep=False)
dup_df = df_keys.loc[
    dup_pairs,
    [FILM_KEY, ALBUM_KEY] + [c for c in ["film_title", "album_title"] if c in df_keys.columns]
]

print("\n[Invariant] ALBUM: No duplicate (tmdb_id, release_group_mbid) pairs")
print("Duplicate pair rows:", int(dup_pairs.sum()))

if dup_pairs.any():
    st.dataframe(dup_df.sort_values([FILM_KEY, ALBUM_KEY]).head(50))

# Hard assert (uncomment when ready)
# assert dup_pairs.sum() == 0, "Invariant failed: duplicate (tmdb_id, release_group_mbid) pairs exist."


# ----------------
# ARTIST SPINE
# ----------------
artist_df = latest_artist_df.copy()
ARTIST_KEY = "artist_mbid" if "artist_mbid" in artist_df.columns else "artist_id"

artist_keys = artist_df.dropna(subset=[ARTIST_KEY]).copy()

dup_artists = artist_keys.duplicated(subset=[ARTIST_KEY], keep=False)
dup_artist_df = artist_keys.loc[
    dup_artists,
    [ARTIST_KEY] + [c for c in ["artist_name", "artist_sort_name"] if c in artist_keys.columns]
]

print(f"\n[Invariant] ARTIST: One row per artist ({ARTIST_KEY})")
print("Duplicate artist-key rows:", int(dup_artists.sum()))

if dup_artists.any():
    st.dataframe(dup_artist_df.sort_values([ARTIST_KEY]).head(50))

# Hard assert
assert dup_artists.sum() == 0, f"Invariant failed: duplicate {ARTIST_KEY} rows exist in artist spine."


# ----------------
# WIDE TRACK TABLE
# ----------------
wide_df = latest_wide_df.copy()

FILM_KEY = "tmdb_id"
ALBUM_KEY = "release_group_id"
TRACK_KEY = "track_id"

wide_keys = wide_df.dropna(subset=[FILM_KEY, ALBUM_KEY, TRACK_KEY]).copy()

dup_triplets_wide = wide_keys.duplicated(subset=[FILM_KEY, ALBUM_KEY, TRACK_KEY], keep=False)
dup_wide_df = wide_keys.loc[
    dup_triplets_wide,
    [FILM_KEY, ALBUM_KEY, TRACK_KEY] + [c for c in ["film_title", "album_title", "track_title"] if c in wide_keys.columns]
]

print(f"\n[Invariant] WIDE: No duplicate ({FILM_KEY}, {ALBUM_KEY}, {TRACK_KEY}) keys")
print("Duplicate key rows:", int(dup_triplets_wide.sum()))

if dup_triplets_wide.any():
    st.dataframe(dup_wide_df.sort_values([FILM_KEY, ALBUM_KEY, TRACK_KEY]).head(50))

# Hard assert
assert dup_triplets_wide.sum() == 0, f"Invariant failed: duplicate ({FILM_KEY}, {ALBUM_KEY}, {TRACK_KEY}) keys exist in wide table."


st.markdown("""
Finding: All three layers pass cleanly — 0 duplicate film↔album pairs, 0 duplicate artist keys, and 0 duplicate film↔track pairs, so the post\\-clean spine keys are behaving as true uniques\\.
""")

st.markdown("""
### II\\.3 Key stability
""")

st.markdown("""
Question: Are the spine keys stable and unique \\(no nulls, no duplicates\\) so downstream joins/aggregations can safely rely on them?
""")

# ============================================================
# 3) Stable and unique spine keys after cleansing (Album + Artist + Wide)
#    Define the “spine key” explicitly and ensure it’s unique & non-null.
# ============================================================

# ----------------
# ALBUM SPINE
# ----------------
df = latest_album_df.copy()
FILM_KEY = "tmdb_id"
ALBUM_KEY = "release_group_mbid"

df_keys = df.dropna(subset=[FILM_KEY, ALBUM_KEY]).copy()

spine_key = df_keys[FILM_KEY].astype("string") + "||" + df_keys[ALBUM_KEY].astype("string")

print("\n[Invariant] ALBUM: Stable & unique spine keys")
print("Spine key nulls:", int(spine_key.isna().sum()))
print("Spine key duplicates:", int(spine_key.duplicated().sum()))

if spine_key.duplicated().any():
    dup_key_idx = spine_key[spine_key.duplicated(keep=False)].index
    display(
        df_keys.loc[
            dup_key_idx,
            [FILM_KEY, ALBUM_KEY] + [c for c in ["film_title", "album_title"] if c in df_keys.columns]
        ]
        .sort_values([FILM_KEY, ALBUM_KEY])
        .head(50)
    )

assert spine_key.isna().sum() == 0, "Invariant failed (ALBUM): spine key contains nulls."
assert spine_key.duplicated().sum() == 0, "Invariant failed (ALBUM): spine key is not unique."


# ----------------
# ARTIST SPINE
# NOTE: Artist spine has no tmdb_id, so the “spine key”
# here is just the artist key (should be one row per artist).
# ----------------
artist_df = latest_artist_df.copy()
ARTIST_KEY = "artist_mbid" if "artist_mbid" in artist_df.columns else "artist_id"

artist_keys = artist_df.dropna(subset=[ARTIST_KEY]).copy()

artist_spine_key = artist_keys[ARTIST_KEY].astype("string")

print("\n[Invariant] ARTIST: Stable & unique spine keys")
print("Spine key nulls:", int(artist_spine_key.isna().sum()))
print("Spine key duplicates:", int(artist_spine_key.duplicated().sum()))

if artist_spine_key.duplicated().any():
    dup_key_idx = artist_spine_key[artist_spine_key.duplicated(keep=False)].index
    display(
        artist_keys.loc[
            dup_key_idx,
            [ARTIST_KEY] + [c for c in ["artist_name", "artist_sort_name"] if c in artist_keys.columns]
        ]
        .sort_values([ARTIST_KEY])
        .head(50)
    )

assert artist_spine_key.isna().sum() == 0, "Invariant failed (ARTIST): spine key contains nulls."
assert artist_spine_key.duplicated().sum() == 0, f"Invariant failed (ARTIST): {ARTIST_KEY} is not unique."


# ----------------
# WIDE TRACK TABLE
# ----------------
wide_df = latest_wide_df.copy()
FILM_KEY = "tmdb_id"
TRACK_KEY = "track_id"  # swap to "recording_mbid" if needed

wide_keys = wide_df.dropna(subset=[FILM_KEY, TRACK_KEY]).copy()

wide_spine_key = wide_keys[FILM_KEY].astype("string") + "||" + wide_keys[TRACK_KEY].astype("string")

print("\n[Invariant] WIDE: Stable & unique spine keys")
print("Spine key nulls:", int(wide_spine_key.isna().sum()))
print("Spine key duplicates:", int(wide_spine_key.duplicated().sum()))

if wide_spine_key.duplicated().any():
    dup_key_idx = wide_spine_key[wide_spine_key.duplicated(keep=False)].index
    display(
        wide_keys.loc[
            dup_key_idx,
            [FILM_KEY, TRACK_KEY] + [c for c in ["film_title", "album_title", "track_title"] if c in wide_keys.columns]
        ]
        .sort_values([FILM_KEY, TRACK_KEY])
        .head(50)
    )

assert wide_spine_key.isna().sum() == 0, "Invariant failed (WIDE): spine key contains nulls."
assert wide_spine_key.duplicated().sum() == 0, "Invariant failed (WIDE): spine key is not unique."

st.markdown("""
# III\\. Canonical soundtrack checks
""")

st.markdown("""
This section evaluates whether canonical soundtrack assignment behaves coherently at scale\\. It confirms that canonical selection is unambiguous at the album level and survives correctly through track\\-level propagation\\.
""")

st.markdown("""
### III\\.1 ALBUM: Canonical soundtrack behavior
""")

st.markdown("""
Question: Does canonicalization produce a coherent universe \\(each film has exactly one canonical soundtrack album, with no zero/multi\\-canonical cases\\)?
""")

# ============================================================
# III. Canonical soundtrack flag behavior (3.4)
#   Source of truth: Album spine (latest_album_df)
#   Downstream consistency: Wide table (latest_wide_df)
#   Optional hygiene: Orphan artists (latest_artist_df vs wide)
# ============================================================

FILM_KEY = "tmdb_id"
ALBUM_KEY = "release_group_mbid"
TRACK_KEY = "track_id"
CANON_COL = "is_canonical_soundtrack"


# ------------------------------------------------------------
# Helper: split a pipe-delimited MBID field into a set
# ------------------------------------------------------------
def split_pipe_mbids(series: pd.Series) -> set:
    """
    Converts a Series of pipe-delimited MBID strings into a set of MBIDs.
    Example cell: "mbid1 | mbid2 | mbid3"
    """
    mbids = set()
    for val in series.dropna().astype(str):
        parts = [p.strip() for p in val.split("|")]
        mbids.update([p for p in parts if p])
    return mbids


# ============================================================
# III.1 Album spine — canonicalization at scale (authoritative)
# ============================================================
album = latest_album_df.copy()

required_album_cols = [FILM_KEY, ALBUM_KEY, CANON_COL]
missing_album_cols = [c for c in required_album_cols if c not in album.columns]
assert len(missing_album_cols) == 0, f"Album spine missing columns: {missing_album_cols}"

# Canonical album count per film (should be exactly 1 for every film in scope)
canon_ct_by_film = (
    album.groupby(FILM_KEY)[CANON_COL]
    .sum()
    .fillna(0)
    .astype(int)
)

films_total = int(canon_ct_by_film.shape[0])
films_exactly_1 = int((canon_ct_by_film == 1).sum())
films_zero = int((canon_ct_by_film == 0).sum())
films_multi = int((canon_ct_by_film > 1).sum())

print("\n=== III.1 ALBUM: Canonical soundtrack behavior (authoritative) ===")
print("Films in album spine:", films_total)
print("Films with exactly 1 canonical soundtrack:", films_exactly_1)
print("Films with 0 canonical soundtracks:", films_zero)
print("Films with >1 canonical soundtracks:", films_multi)

# Show a small offender sample if anything is off
if films_zero > 0 or films_multi > 0:
    offenders = canon_ct_by_film[(canon_ct_by_film == 0) | (canon_ct_by_film > 1)].index
    cols = [FILM_KEY, ALBUM_KEY, CANON_COL] + [c for c in ["film_title", "album_title"] if c in album.columns]
    display(
        album.loc[album[FILM_KEY].isin(offenders), cols]
        .drop_duplicates()
        .sort_values([FILM_KEY, ALBUM_KEY])
        .head(50)
    )

# Distribution: candidate albums per film vs canonical selection
candidate_albums_per_film = album.groupby(FILM_KEY)[ALBUM_KEY].nunique()
canonical_albums_per_film = canon_ct_by_film

print("\nCandidate albums per film — describe")
st.dataframe(candidate_albums_per_film.describe())

print("\nCanonical albums per film — value counts (should be all 1s)")
st.write(canonical_albums_per_film.value_counts().sort_index())

st.markdown("""
Findings: Canonicalization behaves cleanly and consistently at scale\\. Every film in the cleaned album spine \\(4,448 total\\) has exactly one canonical soundtrack, with no films missing a canonical album and no films assigned multiple canonicals\\. While most films map to a single candidate soundtrack album, a small minority have multiple candidates \\(up to five\\), yet the canonical selection remains unambiguous in all cases\\. This confirms that the canonical universe is well\\-formed, deterministic, and robust to upstream ambiguity, and is safe to treat as the authoritative album\\-level baseline for downstream analysis\\.
""")

st.markdown("""
### III\\.2 WIDE: Canonical survivorship
""")

st.markdown("""
Question: Do canonical albums actually survive into the WIDE table with track rows \\(i\\.e\\., canonical selection isn’t “empty” downstream\\)?
""")

WIDE_ALBUM_ARTIST_MBIDS_COL = "album_artist_mbids_text"   # pipe-delimited MBIDs in wide

# ============================================================
# III.2 Wide table — canonical survivorship (should have tracks)
# ============================================================
wide = latest_wide_df.copy()

required_wide_cols = [FILM_KEY, ALBUM_KEY, TRACK_KEY, CANON_COL]
missing_wide_cols = [c for c in required_wide_cols if c not in wide.columns]
assert len(missing_wide_cols) == 0, f"Wide table missing columns: {missing_wide_cols}"

# Canonical film→album pairs from ALBUM spine (source of truth)
canon_pairs = (
    album.loc[album[CANON_COL] == 1, [FILM_KEY, ALBUM_KEY]]
    .drop_duplicates()
)

# Join to wide and count track coverage
canon_pairs_wide = canon_pairs.merge(
    wide[[FILM_KEY, ALBUM_KEY, TRACK_KEY]],
    on=[FILM_KEY, ALBUM_KEY],
    how="left"
)

# For each film, does its canonical album have ≥1 track row in wide?
has_tracks = canon_pairs_wide.groupby(FILM_KEY)[TRACK_KEY].apply(lambda s: s.notna().any())

films_canon_total = int(has_tracks.shape[0])
films_canon_with_tracks = int(has_tracks.sum())
films_canon_no_tracks = int((~has_tracks).sum())

print("\n=== III.2 WIDE: Canonical survivorship (canonical album has tracks) ===")
print("Films with canonical album (from album spine):", films_canon_total)
print("…canonical album has ≥1 track row in wide:", films_canon_with_tracks)
print("…canonical album has 0 track rows in wide:", films_canon_no_tracks)

if films_canon_no_tracks > 0:
    missing_film_ids = has_tracks[~has_tracks].index
    display(
        canon_pairs.loc[canon_pairs[FILM_KEY].isin(missing_film_ids)]
        .merge(album[[FILM_KEY, "film_title"]].drop_duplicates(), on=FILM_KEY, how="left")
        .sort_values([FILM_KEY])
        .head(50)
    )

st.markdown("""
Findings: Canonical albums largely survive downstream into the wide track table as expected\\. Of the 4,448 films with a canonical soundtrack defined at the album level, 4,437 \\(99\\.8%\\) have at least one associated track row in the wide table, confirming that canonical selections are almost always represented at the track level\\. A small residual set of 11 films \\(~0\\.25%\\) have canonical albums with no surviving track rows, indicating limited edge cases where album\\-level canonicals do not propagate fully through track joins or filtering\\. These cases are rare, isolated, and suitable for targeted inspection, but they do not undermine the overall coherence of the canonical universe\\.
""")

st.markdown("""
### III\\.3 Orphan artist analysis
""")

st.markdown("""
Question: Are there artists present in the ARTIST spine that never show up in WIDE album\\-artist MBIDs \\(visibility into coverage gaps, not a blocker\\)?
""")

st.dataframe(latest_artist_df.columns)

ARTIST_KEY = "artist_mbid"     # in artist spine CSV
ARTIST_NAME_COL = "name"       # display name

# ============================================================
# III.3 OPTIONAL: Orphan artists (artist spine vs wide)
#   Not a dealbreaker — just visibility.
# ============================================================
artist = latest_artist_df.copy()

artist_set = set(artist[ARTIST_KEY].dropna().astype(str).unique())
wide_artist_set = split_pipe_mbids(wide[WIDE_ALBUM_ARTIST_MBIDS_COL])

orphan_artists = artist_set - wide_artist_set

print("\n=== III.3: Orphan artists (artist spine absent from wide album artists) ===")
print("Artist spine artists:", len(artist_set))
print("Artists present in wide (album artists):", len(wide_artist_set))
print("Orphan artists:", len(orphan_artists))
print("Pct orphan artists:", round(len(orphan_artists) / max(len(artist_set), 1), 4))

# Optional: sample orphans for a quick gut check
if len(orphan_artists) > 0:
    display(
        artist.loc[
            artist[ARTIST_KEY].astype(str).isin(list(orphan_artists)),
            [ARTIST_KEY, ARTIST_NAME_COL]
        ]
        .head(50)
    )

st.markdown("""
Findings: A small number of artists in the artist spine do not appear in the wide table after cleansing\\. Of 2,430 artists in the artist spine, 2,331 \\(95\\.9%\\) are represented among album\\-level artists in the wide table, leaving 99 orphan artists \\(~4\\.1%\\)\\. This outcome is expected given that the artist spine was not subject to the same pruning steps applied to albums and tracks in Sections 3\\.2–3\\.3, and it does not indicate a structural failure\\. Instead, these orphans highlight a modest amount of upstream artist metadata that no longer participates in the cleaned film–album–track universe and can be safely ignored or revisited in future refinement passes if needed\\.
""")

st.markdown("""
# IV\\. Genre and composer check
""")

st.markdown("""
This section performs a light\\-touch sanity check on genre and composer derivation\\. The goal is to ensure genre expansion and composer reconciliation behaved as expected without introducing data corruption, unintended row duplication or distorting the spine\\.
""")

st.markdown("""
### IV\\.1\\.1 ALBUM: Genre explosion sanity
""")

st.markdown("""
Question: Did genre explosion propagate as expected at the album level without distorting the album spine grain?
""")

# ============================================================
# IV.1 Genre explosion sanity (3.5)
#   Goals:
#     1) Expected multiplicative behavior (avg genres per entity)
#     2) No accidental row duplication in core spine tables
# ============================================================

# ------------------------------------------------------------
# Helper: count binary genre flags per row
# ------------------------------------------------------------
def count_flags(df, flag_cols):
    return df[flag_cols].fillna(0).astype(int).sum(axis=1)


# ============================================================
# IV.1.1 ALBUM SPINE — genre flags sanity
# ============================================================
album = latest_album_df.copy()

ALBUM_GENRE_COLS = [
    "ambient_experimental",
    "classical_orchestral",
    "electronic",
    "hip_hop_rnb",
    "pop",
    "rock",
    "world_folk",
]

print("\n=== IV.1.1 ALBUM: Genre explosion sanity ===")

# Avg genres per album
album_genre_ct = count_flags(album, ALBUM_GENRE_COLS)

print("Albums:", len(album))
print("Avg genres per album:", round(album_genre_ct.mean(), 3))
print("Albums with ≥1 genre:", int((album_genre_ct > 0).sum()))
print("Albums with 0 genres:", int((album_genre_ct == 0).sum()))

# Rowcount + key sanity (light check only)
print("Unique albums (release_group_mbid):", album["release_group_mbid"].nunique())

st.markdown("""
Findings: Album\\-level genre expansion behaves conservatively, with only ~25% of albums carrying any genre flags and an average of 0\\.25 genres per album\\. The album spine remains one row per release group, indicating no unintended row multiplication during genre derivation\\.
""")

st.markdown("""
### IV\\.1\\.2 ARTIST: Genre explosion sanity
""")

st.markdown("""
Question: Did genre explosion propagate as expected at the artist level without inflating or distorting the artist spine grain?
""")

# ============================================================
# IV.1.2 ARTIST SPINE — genre flags sanity
# ============================================================
artist = latest_artist_df.copy()

ARTIST_GENRE_COLS = [
    "electronic_producer",
    "jazz_traditional",
    "media_composer_orchestral",
    "pop_vocalist",
    "rock_artist_band",
    "world_folk_regional",
]

print("\n=== IV.1.2 ARTIST: Genre explosion sanity ===")

artist_genre_ct = count_flags(artist, ARTIST_GENRE_COLS)

print("Artists:", len(artist))
print("Avg genres per artist:", round(artist_genre_ct.mean(), 3))
print("Artists with ≥1 genre:", int((artist_genre_ct > 0).sum()))
print("Artists with 0 genres:", int((artist_genre_ct == 0).sum()))

# Artist spine should remain one row per artist
print("Unique artists (artist_mbid):", artist["artist_mbid"].nunique())

st.markdown("""
Findings: Artist genre tagging remains sparse and well\\-behaved, with ~29% of artists assigned at least one genre and an average of 0\\.41 genres per artist\\. The artist spine retains strict one\\-row\\-per\\-artist integrity, confirming genre flags did not distort artist\\-level structure\\.
""")

st.markdown("""
### IV\\.1\\.3 WIDE: Genre explosion sanity
""")

st.markdown("""
Question: Did genre propagation into WIDE behave multiplicatively \\(more flags per row\\) without creating accidental row duplication?
""")

print("\n=== IV.1.3 WIDE: Genre explosion sanity ===")

# ============================================================
# IV.1.3 WIDE TABLE — genre propagation sanity
# ============================================================
wide = latest_wide_df.copy()

WIDE_ALBUM_GENRE_COLS = [
    "album_ambient_experimental",
    "album_classical_orchestral",
    "album_electronic",
    "album_hip_hop_rnb",
    "album_pop",
    "album_rock",
    "album_world_folk",
]

WIDE_ARTIST_GENRE_COLS = [
    "Artist_media_composer_orchestral",
    "Artist_pop_vocalist",
    "Artist_rock_artist_band",
    "Artist_electronic_producer",
    "Artist_jazz_traditional",
    "Artist_world_folk_regional",
]

# Album genres per row
wide_album_genre_ct = count_flags(wide, WIDE_ALBUM_GENRE_COLS)
print("Avg album genres per wide row:", round(wide_album_genre_ct.mean(), 3))

# Artist genres per row
wide_artist_genre_ct = count_flags(wide, WIDE_ARTIST_GENRE_COLS)
print("Avg artist genres per wide row:", round(wide_artist_genre_ct.mean(), 3))

# Core grain sanity (no accidental row multiplication)
print("Wide rows:", len(wide))
print("Unique tracks (track_id):", wide["track_id"].nunique())
print("Unique film–album–track keys:",
      wide[["tmdb_id", "release_group_mbid", "track_id"]].drop_duplicates().shape[0])

st.markdown("""
Findings\\. Genre information propagates cleanly into the wide table, with modest album\\-genre density and higher artist\\-genre density per track, as expected given multi\\-artist credits\\. Core wide\\-table grain remains intact, with no evidence of row duplication or key distortion following genre expansion\\.
""")

st.markdown("""
### IV\\.2\\.1 ALBUM: Composer coverage
""")

st.markdown("""
Question: What % of film–album rows have usable composer\\_primary\\_clean, and what % remain Unknown?
""")

print("\n=== IV.2.1 ALBUM: Composer coverage (film–album rows) ===")

# ============================================================
# IV.2.1 ALBUM TABLE — composer_primary_clean coverage snapshot
#
# Goal:
#   At film–album grain (tmdb_id, release_group_id), quantify:
#     - % of rows with composer_primary_clean populated (not Unknown / not null)
#     - % of rows still Unknown
#
# This is a visibility check (not a correctness proof).
# ============================================================

album = latest_album_df.copy()

required_cols = ["tmdb_id", "release_group_id", "composer_primary_clean"]
missing = [c for c in required_cols if c not in album.columns]
assert not missing, f"latest_album_df missing required columns: {missing}"

# Work at film–album grain (one row per tmdb_id + release_group_id)
album_keyed = (
    album[["tmdb_id", "release_group_id", "film_title", "film_year", "composer_primary_clean"]]
    .drop_duplicates(subset=["tmdb_id", "release_group_id"])
)

# Normalize the composer field for consistent checks
composer = album_keyed["composer_primary_clean"].astype(str).str.strip()
is_unknown = composer.eq("Unknown") | composer.eq("nan") | composer.eq("") | composer.isna()

total = len(album_keyed)
unknown_ct = int(is_unknown.sum())
populated_ct = total - unknown_ct

def pct(n, d):
    return round((n / d) * 100, 2) if d else 0.0

print("Film–album rows (distinct tmdb_id+release_group_id):", total)
print("Composer populated:", populated_ct, f"({pct(populated_ct, total)}%)")
print("Composer Unknown/blank:", unknown_ct, f"({pct(unknown_ct, total)}%)")

unknown_films = album_keyed.loc[is_unknown, "tmdb_id"].nunique()
total_films = album_keyed["tmdb_id"].nunique()

def pct(n, d):
    return round((n / d) * 100, 2) if d else 0.0

print(
    "Distinct films represented in Unknown rows:",
    unknown_films,
    f"({pct(unknown_films, total_films)}%)"
)


# Quick gut-check sample of Unknowns
if unknown_ct > 0:
    print("\nSample film–album rows where composer_primary_clean is Unknown:")
    display(
        album_keyed.loc[is_unknown, ["tmdb_id", "release_group_id", "film_title", "film_year", "composer_primary_clean"]]
        .head(50)
    )

st.markdown("""
Findings: Across 4,771 distinct film–album rows, 98\\.1% have a populated composer\\_primary\\_clean, indicating near\\-complete primary composer coverage after integrating TMDB and MusicBrainz data\\. Only 1\\.9% \\(91 film–album rows\\) remain marked as Unknown\\.
""")

st.markdown("""
These Unknowns are concentrated across 88 distinct films \\(2\\.0%\\), suggesting they represent genuine attribution edge cases—such as licensed\\-music\\-driven soundtracks or compilation\\-style albums—rather than systemic data quality or merge issues\\. Spot checks confirm that remaining Unknowns align with films where a single canonical score composer is ambiguous or not consistently defined\\.
""")

st.markdown("""
Overall, the composer column is high\\-coverage, stable, and analysis\\-ready, with remaining Unknowns forming a small, coherent, and explainable subset\\.
""")

st.markdown("""
### IV\\.2\\.2 ALBUM: MB fallback availability when TMDB composer is Unknown
""")

st.markdown("""
Question: When TMDB composer is Unknown, how often does MusicBrainz provide a usable album\\-level fallback signal?
""")

print("\n=== IV.2.2 ALBUM: MB fallback availability when TMDB composer is Unknown ===")

# ============================================================
# IV.2.2 ALBUM TABLE — MB fallback availability (ALBUM GRAIN)
#
# Goal:
#   When TMDB composer is 'Unknown' at film–album grain, quantify:
#     - how many of those album keys have a usable MB position=0 credit
#       (excluding 'Various Artists').
#
# Notes:
#   - This does NOT re-evaluate final composer_primary_clean coverage (IV.2.1).
#   - This is about *source availability* for recovery, not final correctness.
# ============================================================

mb_bridge_path = "./pipeline/2.2.MUSICBRAINZ_mv_tmdb_soundtrack_album_artist_bridge_2015_2025.csv"
mb = pd.read_csv(mb_bridge_path)

def pct(n, d):
    return round((n / d) * 100, 2) if d else 0.0

# Reuse album_keyed from IV.2.1 if available; otherwise compute it quickly
album = latest_album_df.copy()
album_keyed = (
    album[["tmdb_id", "release_group_id", "film_soundtrack_composer_raw"]]
    .drop_duplicates(["tmdb_id", "release_group_id"])
)

# Album keys where TMDB composer is Unknown (this is the *input* missingness)
tmdb_unknown = album_keyed["film_soundtrack_composer_raw"].astype(str).str.strip().eq("Unknown")
unknown_keys_df = album_keyed.loc[tmdb_unknown, ["tmdb_id", "release_group_id"]].drop_duplicates()

# MB usable primary keys (pos0 non-VA) at album grain
mb_pos0_nonva_keys = (
    mb[(mb["position"] == 0) & (mb["credited_name"] != "Various Artists")]
    [["tmdb_id", "release_group_id"]]
    .drop_duplicates()
)

# Left join to see coverage of MB fallback among TMDB-Unknown album keys
unknown_with_mb = unknown_keys_df.merge(
    mb_pos0_nonva_keys,
    on=["tmdb_id", "release_group_id"],
    how="inner",
    validate="m:1"
)

total_unknown = len(unknown_keys_df)
covered = len(unknown_with_mb)

print("Album keys where TMDB composer is Unknown:", total_unknown, f"({pct(total_unknown, len(album_keyed))}%)")
print("...of those, MB fallback available (pos0 non-VA):", covered, f"({pct(covered, total_unknown)}%)")
print("...remaining with no usable MB fallback:", total_unknown - covered, f"({pct(total_unknown - covered, total_unknown)}%)")

st.markdown("""
Findings: At the film–album level, 16\\.8% of albums \\(800 keys\\) have the composer listed as “Unknown” in TMDB\\. For the large majority of these cases, MusicBrainz provides a usable primary album credit: 709 of the 800 Unknown albums \\(88\\.6%\\) have a non\\-“Various Artists” position\\-0 credit available via the album–artist bridge\\.
""")

st.markdown("""
Only 91 album keys \\(11\\.4% of TMDB\\-Unknown cases; 1\\.9% of the full album spine\\) lack a usable MusicBrainz fallback\\. These represent albums where a single canonical composer is genuinely absent or ambiguous, rather than a failure of the enrichment or merge logic\\.
""")

st.markdown("""
Taken together with IV\\.2\\.1, this confirms that MusicBrainz materially reduces composer missingness while preserving Unknowns only in structurally appropriate edge cases\\.
""")

st.markdown("""
### IV\\.2\\.3 WIDE↔ALBUM: Composer consistency at album grain
""")

st.markdown("""
Question: Did composer\\_primary\\_clean merge into WIDE correctly at album grain \\(one composer per album key, and identical values between ALBUM and WIDE\\)?
""")

print("\n=== IV.2.3 WIDE↔ALBUM: Composer consistency at album grain ===")

# ============================================================
# IV.2.3 WIDE↔ALBUM — composer propagation consistency
#
# Goal:
#   Confirm that composer_primary_clean propagates correctly from
#   the ALBUM table to the WIDE table at album grain
#   (tmdb_id, release_group_id).
#
# What this guards against:
#   - Merging composers at film grain instead of album grain
#   - Silent row multiplication or key mismatch during WIDE merge
#   - Inconsistent composer values across tracks for the same album
#
# Expected outcome:
#   - Exactly one composer per album key in WIDE
#   - Zero mismatches between ALBUM and WIDE composer values
# ============================================================

album = latest_album_df.copy()
wide = latest_wide_df.copy()

def norm(s):
    return s.astype(str).str.strip()

# One row per album key from ALBUM
album_keyed = (
    album[["tmdb_id", "release_group_id", "composer_primary_clean"]]
    .drop_duplicates(["tmdb_id", "release_group_id"])
    .copy()
)
album_keyed["composer_album"] = norm(album_keyed["composer_primary_clean"])

# One row per album key from WIDE (composer should be constant across tracks)
wide_keyed = (
    wide[["tmdb_id", "release_group_id", "composer_primary_clean"]]
    .drop_duplicates(["tmdb_id", "release_group_id", "composer_primary_clean"])
    .copy()
)
wide_keyed["composer_wide"] = norm(wide_keyed["composer_primary_clean"])

# 1) Guardrail: WIDE should not have >1 distinct composer per album key
wide_multi = (
    wide_keyed.groupby(["tmdb_id", "release_group_id"])["composer_wide"]
    .nunique()
    .reset_index(name="distinct_composers_in_wide")
)
wide_multi = wide_multi[wide_multi["distinct_composers_in_wide"] > 1]

print("Album keys with >1 distinct composer in WIDE:", len(wide_multi))
if len(wide_multi) > 0:
    st.write(wide_multi.head(25))

# 2) Match check: ALBUM composer should equal WIDE composer for the same album key
cmp = album_keyed.merge(
    wide_keyed.drop_duplicates(["tmdb_id", "release_group_id"]),
    on=["tmdb_id", "release_group_id"],
    how="left",
    validate="1:1"
)

cmp["matches"] = cmp["composer_album"] == cmp["composer_wide"]
mismatch = cmp[~cmp["matches"] & cmp["composer_wide"].notna()]

print("Album keys compared:", len(cmp))
print("Mismatches (ALBUM vs WIDE):", len(mismatch))

if len(mismatch) > 0:
    st.write(mismatch.head(25))

st.markdown("""
Findings: Composer values are fully consistent between the ALBUM and WIDE tables at album grain\\. Each album maps to exactly one composer in WIDE, with zero cases of conflicting composer values and no mismatches between ALBUM and WIDE\\. This confirms that the composer merge was performed at the correct grain and did not introduce drift or duplication\\.
""")

st.markdown("""
# V\\. Impact of filter flags
""")

st.markdown("""
This section previews the impact of applying key scoping flags before they are enforced\\. It shows how a vote\\-count threshold and canonical soundtrack selection would shape the analysis universe by year and in combination, providing a clear sense of coverage and scale without reducing or filtering the underlying data\\.
""")

st.markdown("""
### V\\.1 Vote threshold coverage
""")

st.markdown("""
Question: If we enforced the vote\\_count threshold, how much of the universe would remain \\(overall and by year\\)?
""")

# ============================================================
# V. Scoping Preview (Flags Only) — 3.6
# Preview of what WOULD remain if we enforced:
#   - vote_count_above_500
#   - is_canonical_soundtrack
# This section does NOT filter or drop rows.
# ============================================================

# -----------------------------
# Helpers
# -----------------------------
def pct(n, d):
    return round(100.0 * n / d, 2) if d else 0.0

def year_coverage_table(df, year_col, flag_col, label, min_year=2015, max_year=2025):
    d = df.copy()

    # Ensure booleans (in case flags are 0/1 or strings)
    d[flag_col] = d[flag_col].astype(bool)

    out = (
        d.groupby(year_col)
         .agg(
             rows=("tmdb_id", "size"),
             films=("tmdb_id", "nunique"),
             flagged_rows=(flag_col, "sum"),
             flagged_films=("tmdb_id", lambda s: d.loc[s.index].groupby("tmdb_id")[flag_col].any().sum())
         )
         .reset_index()
         .rename(columns={year_col: "film_year"})
    )

    out["flagged_rows_pct"] = out.apply(lambda r: pct(r["flagged_rows"], r["rows"]), axis=1)
    out["flagged_films_pct"] = out.apply(lambda r: pct(r["flagged_films"], r["films"]), axis=1)

    # Keep the intended year window if present
    if "film_year" in out.columns:
        out = out[(out["film_year"] >= min_year) & (out["film_year"] <= max_year)]

    print(f"\n=== V.1 {label}: Coverage of {flag_col} by year (rows + films) ===")
    st.write(out.sort_values("film_year"))

    # Overall summary
    total_rows = len(d)
    total_films = d["tmdb_id"].nunique()
    flagged_rows = int(d[flag_col].sum())
    flagged_films = int(d.groupby("tmdb_id")[flag_col].any().sum())

    print(f"\nOverall ({label})")
    print(f"Rows flagged: {flagged_rows} / {total_rows} ({pct(flagged_rows, total_rows)}%)")
    print(f"Films flagged: {flagged_films} / {total_films} ({pct(flagged_films, total_films)}%)")


# ============================================================
# V.1 Coverage of vote_count_above_500 by year and overall
# ============================================================

# Album grain (one row per film–album link)
year_coverage_table(
    df=latest_album_df,
    year_col="film_year",
    flag_col="vote_count_above_500",
    label="ALBUM (album spine)"
)

# Wide grain (one row per film–album–track link)
year_coverage_table(
    df=latest_wide_df,
    year_col="film_year",
    flag_col="vote_count_above_500",
    label="WIDE (track table)"
)

st.markdown("""
Findings: Roughly 36–37% of films in the cleaned spine meet the vote\\_count \\> 500 threshold overall, with coverage declining in more recent years as newer releases have had less time to accumulate votes\\. This pattern reflects an expected survivorship bias introduced by applying a fixed vote threshold: older films are more likely to qualify simply due to longer exposure\\. At the track \\(wide\\) level, a larger share of rows \\(~46%\\) are associated with higher\\-vote films, driven by deeper track inventories for popular titles rather than increased film\\-level coverage\\. This bias is a known and intentional scoping tradeoff surfaced here for transparency\\.
""")

st.markdown("""
### V\\.2 Canonical × popularity overlap
""")

st.markdown("""
Question: How much overlap exists between “canonical soundtrack” and “vote\\_count\\_above\\_500,” and what would we keep/lose by scoping?
""")

# ============================================================
# V.2 Overlap: vote_count_above_500 AND is_canonical_soundtrack
# (evaluated at album grain; wide is optional propagation view)
# ============================================================

print("\n=== V.2 ALBUM: Overlap of vote_count_above_500 and is_canonical_soundtrack ===")
a = latest_album_df.copy()
a["vote_count_above_500"] = a["vote_count_above_500"].astype(bool)
a["is_canonical_soundtrack"] = a["is_canonical_soundtrack"].astype(bool)

# Film-level flags (does the film have at least one canonical album? does it meet vote threshold?)
film_vote = a.groupby("tmdb_id")["vote_count_above_500"].any()
film_has_canon = a.groupby("tmdb_id")["is_canonical_soundtrack"].any()

overlap = pd.DataFrame({
    "vote_count_above_500": film_vote,
    "has_canonical_soundtrack": film_has_canon
}).reset_index()

# 2x2 counts
ct = (
    overlap.groupby(["vote_count_above_500", "has_canonical_soundtrack"])
           .size()
           .reset_index(name="film_ct")
           .sort_values(["vote_count_above_500", "has_canonical_soundtrack"])
)

total_films = overlap["tmdb_id"].nunique()
ct["pct_films"] = ct["film_ct"].apply(lambda x: pct(x, total_films))

st.write(ct)

print("\nQuick read:")
print(f"Total films (album spine): {total_films}")
print(f"Films vote_count_above_500: {int(overlap['vote_count_above_500'].sum())}")
print(f"Films with ≥1 canonical soundtrack: {int(overlap['has_canonical_soundtrack'].sum())}")
print(f"Films with BOTH: {int(((overlap['vote_count_above_500']) & (overlap['has_canonical_soundtrack'])).sum())}")


# Optional: wide-table propagation view (film-level)
print("\n=== V.2 WIDE (optional): Overlap at film-level in wide table ===")
w = latest_wide_df.copy()
w["vote_count_above_500"] = w["vote_count_above_500"].astype(bool)
w["is_canonical_soundtrack"] = w["is_canonical_soundtrack"].astype(bool)

w_film_vote = w.groupby("tmdb_id")["vote_count_above_500"].any()
w_film_has_canon = w.groupby("tmdb_id")["is_canonical_soundtrack"].any()

w_overlap = pd.DataFrame({
    "vote_count_above_500": w_film_vote,
    "has_canonical_soundtrack": w_film_has_canon
}).reset_index()

w_ct = (
    w_overlap.groupby(["vote_count_above_500", "has_canonical_soundtrack"])
             .size()
             .reset_index(name="film_ct")
             .sort_values(["vote_count_above_500", "has_canonical_soundtrack"])
)
w_total_films = w_overlap["tmdb_id"].nunique()
w_ct["pct_films"] = w_ct["film_ct"].apply(lambda x: pct(x, w_total_films))
st.write(w_ct)



st.markdown("""
Findings: All 4,448 films in the album spine have a canonical soundtrack, and 1,622 films \\(36\\.47%\\) also meet the vote\\_count \\> 500 threshold, confirming that vote count functions purely as an additive scoping flag\\. The relationship is effectively preserved in the wide table, with two transient edge cases \\(~0\\.04%\\) that will be removed when constructing the final canonical analytics set\\.
""")

st.markdown("""
### V\\.3 Scoped universe preview
""")

st.markdown("""
Question: What would the final dataset look like if scoping flags were enforced \\(size and characteristics\\), without actually filtering yet?
""")

# ============================================================
# V.3 Preview size of the universe if scoping WERE enforced
# (still preview only; no filtering of the stored dfs)
# ============================================================

print("\n=== V.3 Preview: If scoping were enforced (flags-only estimate) ===")

# Album-scoped universe: canonical albums AND vote_count_above_500 at film level
a_film_flags = overlap.set_index("tmdb_id")
scoped_films = set(a_film_flags.index[(a_film_flags["vote_count_above_500"]) & (a_film_flags["has_canonical_soundtrack"])])

# Within album spine, what rows/albums would remain if we kept only canonical albums for scoped films?
a_scoped = a[a["tmdb_id"].isin(scoped_films)].copy()
a_scoped_canon = a_scoped[a_scoped["is_canonical_soundtrack"]].copy()

print("Album spine (preview counts)")
print("Scoped films (vote>500 AND has canonical):", len(scoped_films))
print("Album rows for scoped films (all albums):", len(a_scoped))
print("Canonical album rows for scoped films:", len(a_scoped_canon))
print("Unique albums (canonical):", a_scoped_canon["release_group_mbid"].nunique())

# Wide-scoped universe: tracks belonging to canonical albums for scoped films
w_scoped = w[w["tmdb_id"].isin(scoped_films)].copy()
w_scoped_canon = w_scoped[w_scoped["is_canonical_soundtrack"]].copy()

print("\nWide table (preview counts)")
print("Wide rows for scoped films (all tracks):", len(w_scoped))
print("Wide rows for scoped films + canonical albums:", len(w_scoped_canon))
print("Unique tracks (scoped + canonical):", w_scoped_canon["track_id"].nunique())

# Optional: “characteristics” snapshots (no deep analysis)
print("\nCharacteristics snapshot (scoped films)")
film_snapshot = (
    latest_album_df[["tmdb_id", "film_year", "film_vote_count"]]
    .drop_duplicates()
    .assign(is_scoped=lambda d: d["tmdb_id"].isin(scoped_films))
)

summary = (
    film_snapshot.groupby("is_scoped")
    .agg(
        films=("tmdb_id", "nunique"),
        avg_vote_count=("film_vote_count", "mean"),
        min_year=("film_year", "min"),
        max_year=("film_year", "max")
    )
    .reset_index()
)
st.write(summary)

st.markdown("""
Findings: If scoping were enforced using vote\\_count \\> 500 and canonical soundtrack flags, the analysis universe would narrow to 1,622 films, each with exactly one canonical album, yielding 1,622 canonical albums and 33,905 associated tracks\\. Compared to the unscoped set, this subset exhibits substantially higher average vote counts while retaining full temporal coverage \\(2015–2025\\), illustrating how scoping concentrates analytical signal without introducing structural distortion\\.
""")

st.markdown("""
# VI\\. Targeted External Plausibility Checks
""")

st.markdown("""
This section performs a small set of targeted, human\\-interpretable plausibility checks against external public sources, intentionally complementing—but not duplicating—the coverage checks in §3\\.6\\. Rather than comparing before/after film count \\> 500 retention within the pipeline, we select a curated set of high\\-signal film composers and compare their publicly documented filmographies \\(2015–2025\\) against the final cleaned spine to confirm that well\\-known real\\-world entities are represented where expected\\. 
""")

st.markdown("""
These checks are illustrative rather than exhaustive and are designed to surface obvious gaps or implausible omissions that system\\-level QA and internal consistency checks may not reveal\\.
""")

st.markdown("""
Question: For known composers with well\\-documented filmographies, do we see the expected titles represented in our ALBUM and WIDE layers \\(coverage sanity vs an external source\\)?
""")

st.markdown("""
### VI\\.1 Helper functions
""")

def _norm_title(s: str) -> str:
    """
    Normalize titles for loose matching:
    - lowercase
    - strip accents
    - remove punctuation
    - collapse whitespace
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def composer_filmography_presence_check(
    composer_name: str,
    source_url: str,
    expected_films_2015_2025: list,
    album_df: pd.DataFrame,
    wide_df: pd.DataFrame,
    film_title_col: str = "film_title",
    vote_flag_col: str = "vote_count_above_500",
    vote_count_col: str = "film_vote_count",   # optional but nice to print
):
    """
    Human plausibility check (non-gating): does the cleaned spine “contain” the films we’d
    reasonably expect for a well-known composer between 2015–2025?

    What it does:
      1) Uses hardcoded expected film titles (from an external source like Wikipedia/IMDb).
      2) Normalizes titles (via _norm_title) to reduce false misses from punctuation/case.
      3) For each expected film, checks:
          - Present in ALBUM layer (latest_album_df) by normalized film_title
          - Present in WIDE layer  (latest_wide_df)  by normalized film_title
          - Whether the film is flagged vote_count_above_500 in each layer
            (useful because the final “canonical analytics set” will filter by this later)

    How to interpret results:
      - Missing in both layers = likely out of dataset scope OR title mismatch.
      - Present but vote_flag False = likely to be filtered out later anyway (not a concern).
      - Present and vote_flag True = should survive into the final analytic universe.

    Notes:
      - This is not an invariant test. Some misses are expected due to title variants,
        alternate releases, international titles, or scope decisions.
    """
    print(f"\n--- Composer sanity check: {composer_name} ---")
    print(f"Source: {source_url}")
    print(f"Expected films (2015–2025): {len(expected_films_2015_2025)}")

    # --- Build lookup maps: normalized title -> (flag, vote_count)
    # If the same normalized title appears multiple times, we treat it as present if ANY row is present,
    # and vote_flag True if ANY row has vote_flag True (most permissive / safest summary).
    album_lookup = {}
    for _, r in album_df[[film_title_col, vote_flag_col, vote_count_col]].dropna(subset=[film_title_col]).iterrows():
        n = _norm_title(r[film_title_col])
        flag = bool(r[vote_flag_col]) if pd.notna(r[vote_flag_col]) else False
        vc = r[vote_count_col] if vote_count_col in album_df.columns else None
        prev = album_lookup.get(n)
        if prev is None:
            album_lookup[n] = {"present": True, "vote_flag": flag, "vote_count": vc}
        else:
            album_lookup[n]["vote_flag"] = album_lookup[n]["vote_flag"] or flag
            # keep the max vote_count if available
            if pd.notna(vc) and (prev["vote_count"] is None or (pd.notna(prev["vote_count"]) and vc > prev["vote_count"])):
                album_lookup[n]["vote_count"] = vc

    wide_lookup = {}
    for _, r in wide_df[[film_title_col, vote_flag_col, vote_count_col]].dropna(subset=[film_title_col]).iterrows():
        n = _norm_title(r[film_title_col])
        flag = bool(r[vote_flag_col]) if pd.notna(r[vote_flag_col]) else False
        vc = r[vote_count_col] if vote_count_col in wide_df.columns else None
        prev = wide_lookup.get(n)
        if prev is None:
            wide_lookup[n] = {"present": True, "vote_flag": flag, "vote_count": vc}
        else:
            wide_lookup[n]["vote_flag"] = wide_lookup[n]["vote_flag"] or flag
            if pd.notna(vc) and (prev["vote_count"] is None or (pd.notna(prev["vote_count"]) and vc > prev["vote_count"])):
                wide_lookup[n]["vote_count"] = vc

    # --- Evaluate each expected film
    rows = []
    for raw_title in expected_films_2015_2025:
        n = _norm_title(raw_title)

        a = album_lookup.get(n, {"present": False, "vote_flag": False, "vote_count": None})
        w = wide_lookup.get(n,  {"present": False, "vote_flag": False, "vote_count": None})

        rows.append({
            "expected_film": raw_title,
            "in_album": a["present"],
            "album_vote>500": a["vote_flag"],
            "album_vote_count": a["vote_count"],
            "in_wide": w["present"],
            "wide_vote>500": w["vote_flag"],
            "wide_vote_count": w["vote_count"],
        })

    results = pd.DataFrame(rows)

    # --- Summary counts
    album_matched = int(results["in_album"].sum())
    wide_matched  = int(results["in_wide"].sum())

    # Later-scope preview: among expected films that are present, how many would survive vote>500?
    album_survive = int((results["in_album"] & results["album_vote>500"]).sum())
    wide_survive  = int((results["in_wide"] & results["wide_vote>500"]).sum())

    missing_both = results.loc[~results["in_album"] & ~results["in_wide"], "expected_film"].tolist()

    print(f"Matched in ALBUM layer: {album_matched} / {len(results)}")
    print(f"Matched in WIDE layer:  {wide_matched} / {len(results)}")
    print(f"Would survive vote_count>500 filter (ALBUM): {album_survive} / {len(results)} (of expected)")
    print(f"Would survive vote_count>500 filter (WIDE):  {wide_survive} / {len(results)} (of expected)")

    if missing_both:
        print(f"Missing in BOTH layers: {len(missing_both)}")
        print("  Missing:", missing_both)

    # --- Display a compact table for humans
    # Put the “interesting” rows first: missing or present-but-filtered-out
    results["_priority"] = (
        (~results["in_album"] & ~results["in_wide"]) * 2 +
        ((results["in_album"] & ~results["album_vote>500"]) | (results["in_wide"] & ~results["wide_vote>500"])) * 1
    )
    display(
        results
        .sort_values(["_priority", "expected_film"], ascending=[False, True])
        .drop(columns=["_priority"])
    )

    return results

st.markdown("""
### VI\\.2 Prestige film composers
""")

# ============================================================
# VI.A — Prestige + awards lane
# (Starter expectations; edit freely if you spot missing titles / variants)
# ============================================================

# Alexandre Desplat (source: https://en.wikipedia.org/wiki/Alexandre_Desplat)
desplat_expected = [
    "The Danish Girl", "Suffragette", "The Little Prince",
    "The Shape of Water", "Isle of Dogs", "Little Women",
    "The Midnight Sky", "The French Dispatch",
    "Guillermo del Toro's Pinocchio", "Asteroid City",
    "The Boys in the Boat"
]

# Nicholas Britell (source: https://en.wikipedia.org/wiki/Nicholas_Britell)
britell_expected = [
    "The Big Short", "Moonlight", "Battle of the Sexes",
    "If Beale Street Could Talk", "The King", "Cruella",
    "Don't Look Up"
]

# Justin Hurwitz (source: https://en.wikipedia.org/wiki/Justin_Hurwitz)
hurwitz_expected = [
    "La La Land", "First Man", "Babylon"
]

composer_filmography_presence_check("Alexandre Desplat", "https://en.wikipedia.org/wiki/Alexandre_Desplat", desplat_expected, latest_album_df, latest_wide_df)
composer_filmography_presence_check("Nicholas Britell", "https://en.wikipedia.org/wiki/Nicholas_Britell", britell_expected, latest_album_df, latest_wide_df)
composer_filmography_presence_check("Justin Hurwitz", "https://en.wikipedia.org/wiki/Justin_Hurwitz", hurwitz_expected, latest_album_df, latest_wide_df)

st.markdown("""
Findings: Coverage for this batch is strong and internally consistent across both the album and wide layers\\. Alexandre Desplat’s filmography shows 10 of 11 expected films present, with 9 of 11 surviving the vote\\_count \\> 500 scoping preview; the single missing title \\(The French Dispatch\\) is a known edge case likely attributable to title or scope differences rather than cleansing loss\\. Nicholas Britell and Justin Hurwitz exhibit complete coverage \\(100%\\), with all expected films present in both layers and all surviving the vote\\-count threshold, reinforcing that the cleaned spine reliably captures high\\-signal, real\\-world composer output in this period\\.
""")

st.markdown("""
### VI\\.3 Franchise and tentpole composers
""")

# ============================================================
# VI.B — Franchise + tentpole lane
# ============================================================

# Michael Giacchino (source: https://en.wikipedia.org/wiki/Michael_Giacchino_discography)
giacchino_expected = [
    "Jupiter Ascending", "Inside Out", "Tomorrowland",
    "Zootopia", "Star Trek Beyond", "Rogue One",
    "Spider-Man: Homecoming", "War for the Planet of the Apes", "Coco",
    "Incredibles 2", "Jurassic World: Fallen Kingdom", "Bad Times at the El Royale",
    "Jojo Rabbit", "Spider-Man: Far From Home",
    "The Batman", "Jurassic World Dominion", "Thor: Love and Thunder"
]

# Ludwig Göransson (source: https://en.wikipedia.org/wiki/Ludwig_G%C3%B6ransson)
goransson_expected = [
    "Creed", "Black Panther", "Tenet", "Turning Red", "Oppenheimer"
]

# Lorne Balfe (source: https://en.wikipedia.org/wiki/Lorne_Balfe)
balfe_expected = [
    "Mission: Impossible – Fallout", "Black Widow", "Top Gun: Maverick",
    "Mission: Impossible – Dead Reckoning Part One", "Argylle"
]

composer_filmography_presence_check("Michael Giacchino", "https://en.wikipedia.org/wiki/Michael_Giacchino_discography", giacchino_expected, latest_album_df, latest_wide_df)
composer_filmography_presence_check("Ludwig Göransson", "https://en.wikipedia.org/wiki/Ludwig_G%C3%B6ransson", goransson_expected, latest_album_df, latest_wide_df)
composer_filmography_presence_check("Lorne Balfe", "https://en.wikipedia.org/wiki/Lorne_Balfe", balfe_expected, latest_album_df, latest_wide_df)

st.markdown("""
Findings:  Coverage remains strong across this batch, with Michael Giacchino showing 15 of 17 expected films present in both the album and wide layers, all of which would survive the vote\\_count \\> 500 scoping preview; the two missing titles \\(Rogue One, The Batman\\) are likely attributable to title\\-matching or scope differences rather than data loss\\. Ludwig Göransson exhibits complete coverage \\(5/5\\) with all films surviving scoping, while Lorne Balfe shows 4 of 5 expected films present, again with all present titles surviving the vote threshold; the single missing film \\(Argylle\\) appears to be an edge case rather than a systemic gap\\.
""")

st.markdown("""
### VI\\. 4 Indie and auteur composers
""")

# ============================================================
# VI.C — Indie + auteur lane
# ============================================================

# Daniel Pemberton (source: https://en.wikipedia.org/wiki/Daniel_Pemberton)
pemberton_expected = [
    "Steve Jobs", "King Arthur: Legend of the Sword",
    "Spider-Man: Into the Spider-Verse", "Enola Holmes",
    "Enola Holmes 2", "Spider-Man: Across the Spider-Verse"
]

# Thomas Newman (source: https://en.wikipedia.org/wiki/Thomas_Newman)
newman_expected = [
    "Spectre", "Bridge of Spies", "Finding Dory",
    "1917", "Elemental"
]

# Carter Burwell (source: https://en.wikipedia.org/wiki/Carter_Burwell)
burwell_expected = [
    "Carol", "Three Billboards Outside Ebbing, Missouri",
    "The Tragedy of Macbeth", "Drive-Away Dolls"
]

composer_filmography_presence_check("Daniel Pemberton", "https://en.wikipedia.org/wiki/Daniel_Pemberton", pemberton_expected, latest_album_df, latest_wide_df)
composer_filmography_presence_check("Thomas Newman", "https://en.wikipedia.org/wiki/Thomas_Newman", newman_expected, latest_album_df, latest_wide_df)
composer_filmography_presence_check("Carter Burwell", "https://en.wikipedia.org/wiki/Carter_Burwell", burwell_expected, latest_album_df, latest_wide_df)

st.markdown("""
Findings: This batch shows complete, clean coverage across all three composers\\. Daniel Pemberton, Thomas Newman, and Carter Burwell each have 100% of expected films present in both the album and wide layers, and all titles survive the vote\\_count \\> 500 scoping preview, indicating no evidence of unexpected omissions or downstream filtering effects for this group\\.
""")
