import streamlit as st
import os, sys

st.set_page_config(page_title="3.1 Investigating Tables", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# Imports
import pandas as pd
import numpy as np
import os
from datetime import datetime

print(os.listdir("./official"))

# Show in decimal rather than scientific notation
pd.options.display.float_format = '{:,.2f}'.format

st.markdown("""
# QA of the Album and Film Table
""")

# Load the albums dataframe

album_df = pd.read_csv("./official/2.1.MUSICBRAINZ_mv_tmdb_soundtrack_album_spine_2015_2025_202601241041.csv")
st.dataframe(album_df.head())

st.dataframe(album_df.columns)

st.markdown("""
## A1\\. Basic Data Integrity Checks
""")

st.markdown("""
### Data Integrity Exploration A1\\.1: Describe numeric columns
""")

st.markdown("""
Question: Is there anything odd about the statistics or ranges around our numeric columns?
""")

# Test A1.1

numeric_album_cols = ['film_vote_count', 'film_rating', 'film_popularity',
'album_us_release_year', 'rg_rating', 'rg_rating_count']

album_df[numeric_album_cols].describe()

st.markdown("""
Album\\-level QA did not reveal material data quality issues; sparsity in U\\.S\\. release dates and MusicBrainz ratings is expected, and given that linkage confidence is already captured by match\\_method, the numeric match\\_score provides little additional analytical value\\.
""")

st.markdown("""
One thing is apparent, though: MusicBrainz ratings can likely be excluded from feature consideration\\. Ratings cluster narrowly between 80 and 100, and rating counts are almost non\\-existent, suggesting limited curator or community engagement with soundtrack rating on the platform\\.
""")

st.markdown("""
### Data Integrity Exploration A1\\.2: ID completeness and uniqueness
""")

st.markdown("""
Question: How complete are our ids?
""")

# Test A1.2 ID completeness and uniqueness
id_columns = ['tmdb_id', 'release_group_id', 'release_group_mbid', 'release_mbid', 'film_imdb_id', 'film_wikidata_id', 'barcode', 'label_mbids']

# Check for missing values in the existing ID columns
missing_values = album_df[id_columns].isna().sum()

# Display the count of missing values for each existing ID column
print(missing_values)
st.dataframe(album_df[id_columns].head())

st.markdown("""
Finding: ID\\-level QA indicates strong join integrity, with complete coverage for primary keys \\(tmdb\\_id, release\\_group\\_id, release\\_group\\_mbid\\) and only expected sparsity across secondary identifiers such as IMDb/Wikidata IDs, barcodes, and label MBIDs \\(which I don't think we will use anyway\\)\\.
""")

dup_ct = album_df.duplicated(subset=["tmdb_id", "release_group_mbid"]).sum()
print("Duplicate (tmdb_id, release_group_mbid) rows:", dup_ct)

st.markdown("""
Finding: Since \\(tmdb\\_id, release\\_group\\_mbid\\) represents are composite primary key for this table, we need to make sure that there are indeed no duplicates\\. Happy to say, there aren't any\\!
""")

st.markdown("""
### Data Integrity Exploration A1\\.3: Categorical coverage and sparsity
""")

st.markdown("""
Question: What are the different values in our categorical variables, and what is the categorical distribution?
""")


# Test A1.3 Categorical coverage and sparsity
# This is more for interest -- I don't think we will be using these columns
categorical_album_cols = ['rg_primary_type', 'release_status', 'release_packaging', 'release_language', 'release_script']

# Check for unique values for those categories
for cat in categorical_album_cols:
    st.write(cat)
    st.dataframe(album_df[cat].value_counts())

st.markdown("""
Finding: Categorical coverage appeared well\\-structured, with long\\-tailed but interpretable values reflecting MusicBrainz taxonomy and international release diversity\\. Despite the U\\.S\\. film\\-release scope, non\\-English languages and non\\-Latin scripts were not treated as deletion criteria and were retained as contextual metadata, with any potential filtering deferred to downstream measurement\\-coverage QA\\. 
""")

st.markdown("""
By contrast, non\\-Official release statuses \\(e\\.g\\., Bootleg, Pseudo\\-Release, Withdrawn\\) should be excluded\\. We will do this in a subsequent cleanup step \\(Notebook 3\\.2: Remove Unofficial Albums\\) to ensure analytical consistency and avoid contaminating popularity and attribution metrics\\.
""")

st.markdown("""
### Data Integrity Exploration A1\\.4: Label exploration
""")

st.markdown("""
Question: How reliable is the label information?
""")

label_cols = ["release_group_mbid", "album_title", "label_names", "label_mbids", "label_tags_text"]
labels_df = album_df.loc[:, label_cols].copy()

st.dataframe(labels_df.isna().sum()/labels_df.shape[0])

st.markdown("""
Finding: Only 12% of albums have null for the label, making them pretty reliable
""")

st.dataframe(labels_df['label_names'].value_counts())
st.dataframe(labels_df['label_names'].value_counts().head(25))

st.markdown("""
Finding: Label metadata is well populated and structurally consistent, with clear concentration among a small number of soundtrack\\-focused and major music labels \\(e\\.g\\., Lakeshore Records, Milan, Back Lot Music, Sony\\)\\. However, the distribution is extremely long\\-tailed, with nearly 1,000 distinct labels and the majority appearing only once\\. In addition, label identity reflects business and distribution relationships rather than musical characteristics\\. As a result, label data is retained as descriptive context but we will not treat it as a primary analytical feature\\.
""")

# %% TMDB composer coverage (album_df)

tmdb_has_composer = (
    album_df["film_soundtrack_composer_raw"].notna()
    & (album_df["film_soundtrack_composer_raw"].astype(str).str.strip() != "")
)

tmdb_film_coverage = (
    album_df.loc[:, ["tmdb_id"]]
       .assign(tmdb_has_composer=tmdb_has_composer)
       .groupby("tmdb_id")["tmdb_has_composer"]
       .any()
)

pd.Series({
    "films_total": int(tmdb_film_coverage.shape[0]),
    "films_with_tmbd_composer": int(tmdb_film_coverage.sum()),
    "pct_films_with_tmdb_composer": float(tmdb_film_coverage.mean() * 100),
})

st.markdown("""
Findings: Composer attribution from TMDB is complete at the film level\\. All films in the album dataset have populated soundtrack composer information in TMDB, yielding 100% coverage\\. This contrasts with the sparse and inconsistent composer metadata observed in MusicBrainz and indicates that TMDB provides a more reliable and structurally aligned source for film\\-level composer analysis\\.
""")

st.markdown("""
## A2\\. Trickier Data Integrity Checks
""")

st.markdown("""
### Data Integrity Exploration A2\\.1: Inspect films with multiple albums
""")

# Test A2.1 Inspect films with multiple albums
multi_films = album_df.groupby('tmdb_id').size()

# Identify films with more than 1 album
multi_films = multi_films[multi_films > 1].index

# display(len(multi_albums))   # 494 multi-albums
cols = [
    "tmdb_id", "film_title", "film_year","film_soundtrack_composer_raw",
    "album_title", "album_us_release_year","rg_secondary_types",
    "soundtrack_type", "match_method",
    "release_group_mbid", "release_mbid"
]

# Identify rows which are in our multi_films list. Show most useful columns for inspection
multi_album_df = album_df.loc[album_df['tmdb_id'].isin(multi_films), cols]. \
    sort_values(['tmdb_id', 'film_title', 'album_us_release_year', 'album_title'])

st.dataframe(multi_album_df)

st.markdown("""
Inspecting films with multiple associated albums shows that “multi\\-album” cases are usually explainable rather than obviously erroneous: many films have both a score release and a songs/soundtrack release \\(e\\.g\\., Ghostbusters, Furious 7, Black Panther\\), and some also include single\\-style releases \\(“Theme From…”, “From the Motion Picture…”\\) that appear as additional album rows\\. 
""")

st.markdown("""
A smaller subset of multi\\-album rows are essentially duplicate/ambiguous album titles \\(often soundtrack\\_type = unknown and/or match\\_method = title\\_contains\\_strict\\), which look like weaker title\\-based associations compared to the imdb\\_exact matches\\. Overall, the output suggests that canonically selecting one album per film will require a clear prioritization rule \\(e\\.g\\., prefer score vs songs, downweight single\\-like and unknown/title\\-only matches\\) rather than treating all multi\\-album films as data quality issues\\.
""")

st.markdown("""
### Data Integrity Exploration A2\\.2: Test Film to album temporal logic
""")


# Test A2.2 Test Film to album temporal logic
album_temporal_df = album_df[['film_title', 'film_year', 'album_title', 'album_us_release_year',
'tmdb_id', 'release_group_mbid', 'release_mbid', 'release_id', 'release_group_id']].copy()

# From the previous test, we know that only 1590 out of the 5328 albums have us_release_dates
album_temporal_df['album_us_release_year'].isna().sum()
# 3738 albums have no U.S. release date

# Test A2.2.1: Are all albums released after the film?
album_temporal_df['album_minus_film_release'] = album_temporal_df['album_us_release_year'] - album_temporal_df['film_year']
album_temporal_df['album_minus_film_release'].value_counts()

st.dataframe(album_temporal_df.sort_values(by='album_minus_film_release', ascending=True).head(50))


st.markdown("""
Most albums have U\\.S\\. release years that coincide with or closely precede the associated film release year, with limited pre\\-release offsets of up to three years\\. Let's confirm the distribution in the next query
""")

# Let's create a table of counts of albums released in the same year as the film, and in the year after the film
st.dataframe(album_temporal_df['album_minus_film_release'].value_counts().sort_index())

# sum all album discrepancies less than -3
np.sum(album_temporal_df['album_minus_film_release'] < -3)

st.markdown("""
As you can see, a vast majority of the albums appear in the same year, or shortly before / after the film release date\\. This means our film / album join data is pretty tight\\.
""")

st.markdown("""
### Data Integrity Exploration A2\\.3: Inspect matches on the film long\\-tail
""")


cols = [
    "tmdb_id", "film_title", "film_year", "film_vote_count",
    "album_title", "album_us_release_year","rg_secondary_types",
    "soundtrack_type", "match_method",
    "release_group_mbid", "release_mbid"
]
mask = (album_df["film_vote_count"] < 100) & (album_df["match_method"] == "title_contains_strict")

st.dataframe(album_df.loc[mask, cols].head(50))

st.markdown("""
Filtering to low–vote\\-count films matched via title\\_contains\\_strict highlights a cluster of album associations with weaker supporting metadata, including frequent missing U\\.S\\. release years and a high prevalence of soundtrack\\_type = unknown, often for generic or repeated titles \\(e\\.g\\., Drive, Red, Luna\\)\\. 
""")

st.markdown("""
These cases are not obviously incorrect, but the title\\-only matching signal is relatively weak compared to IMDb\\-exact matches or albums explicitly labeled as scores or soundtracks\\. As a result, these rows are retained but flagged as lower\\-confidence associations to be handled cautiously in downstream analysis rather than removed outright\\.
""")

st.markdown("""
### Data Integrity Exploration A2\\.4: Non\\-Latin Character Sets
""")

import pandas as pd

# --- Helpers -----------------------------------------------------------------

def contains_any_in_ranges(s, ranges):
    """
    Return True if any character in s falls within any inclusive Unicode range
    in `ranges` (list of (start, end) codepoint tuples).
    """
    for ch in s:
        cp = ord(ch)
        for start, end in ranges:
            if start <= cp <= end:
                return True
    return False


# --- Ranges ------------------------------------------------------------------
# “Non-Latin” scripts we want to catch explicitly (then everything else non-latinish also folds in)
NON_LATIN_RANGES = [
    # CJK + Hangul
    (0x4E00, 0x9FFF),   # Han
    (0x3400, 0x4DBF),   # Han Ext A
    (0x3040, 0x309F),   # Hiragana
    (0x30A0, 0x30FF),   # Katakana
    (0xAC00, 0xD7AF),   # Hangul syllables
    (0x1100, 0x11FF),   # Hangul Jamo

    # Indic (fold into non_latin_script)
    (0x0900, 0x097F),   # Devanagari
    (0x0980, 0x09FF),   # Bengali
    (0x0A00, 0x0A7F),   # Gurmukhi
    (0x0A80, 0x0AFF),   # Gujarati
    (0x0B00, 0x0B7F),   # Oriya
    (0x0B80, 0x0BFF),   # Tamil
    (0x0C00, 0x0C7F),   # Telugu
    (0x0C80, 0x0CFF),   # Kannada
    (0x0D00, 0x0D7F),   # Malayalam

    # Thai
    (0x0E00, 0x0E7F),   # Thai
]

# Accented Latin letters
LATIN_EXT_RANGES = [
    (0x00C0, 0x00FF),  # Latin-1 Supplement
    (0x0100, 0x017F),  # Latin Extended-A
    (0x0180, 0x024F),  # Latin Extended-B
    (0x1E00, 0x1EFF),  # Latin Extended Additional
]

# “Latin-ish” extras that commonly appear in otherwise-Latin titles
# (smart quotes, non-breaking hyphens, roman numerals, ™/®,
# some fullwidth punctuation, etc.)
LATINISH_MISC_RANGES = [
    (0x0300, 0x036F),  # Combining diacritics
    (0x2000, 0x206F),  # General Punctuation (smart quotes, en-dash, etc.)
    (0x2100, 0x214F),  # Letterlike Symbols (™ etc.)
    (0x2150, 0x218F),  # Number Forms (Roman numerals like ⅩⅢ)
    (0x2460, 0x24FF),  # Enclosed Alphanumerics
    (0xFF00, 0xFFEF),  # Halfwidth/Fullwidth forms (includes ～)
    # NOTE: I intentionally removed (0x3000, 0x303F) because it includes 「」,
    # which are Japanese punctuation and you probably want those to count as non_latin_script.
]


def is_latinish_char(ch):
    """
    True if ch is ASCII OR falls into an allowed Latin/typographic block that we
    still consider 'Latin titles' for lookup purposes.
    """
    cp = ord(ch)

    # ASCII
    if cp < 128:
        return True

    # Accented Latin letters
    for start, end in LATIN_EXT_RANGES:
        if start <= cp <= end:
            return True

    # Typographic / symbol blocks we’re treating as still "Latin-ish"
    for start, end in LATINISH_MISC_RANGES:
        if start <= cp <= end:
            return True

    return False


def classify_title_script(text):
    """
    Buckets:
      - ascii_clean:        all characters are ASCII (best for naive lookups)
      - latin_typographic:  Latin text with diacritics and/or typographic punctuation/symbols
                            (usually fine, but normalize smart quotes/hyphens)
      - non_latin_script:   contains non-Latin scripts (Thai/CJK/Indic/etc) OR contains
                            other characters we don’t consider “latin-ish”
    """
    if pd.isna(text) or str(text).strip() == "":
        return "missing"

    s = str(text)

    # 1) Clean ASCII
    if all(ord(c) < 128 for c in s):
        return "ascii_clean"

    # 2) Known non-Latin scripts => higher risk bucket
    if contains_any_in_ranges(s, NON_LATIN_RANGES):
        return "non_latin_script"

    # 3) Everything else: if fully "latin-ish" => typographic bucket, otherwise non-Latin
    return "latin_typographic" if all(is_latinish_char(c) for c in s) else "non_latin_script"


# --- Prevalence (album-level, deduped) ---------------------------------------

album_level = album_df[["release_group_mbid", "album_title"]].drop_duplicates()

script_bucket = album_level["album_title"].apply(classify_title_script)

summary = (
    script_bucket.value_counts(dropna=False)
    .rename("album_count")
    .to_frame()
)
summary["pct_of_albums"] = (summary["album_count"] / summary["album_count"].sum() * 100).round(2)

st.write(summary)

# Inspect examples for each bucket
for bucket in ["latin_typographic", "non_latin_script"]:
    print(f"\nExamples: {bucket}")
    st.write(album_level.loc[script_bucket == bucket].sample(10))

st.markdown("""
The vast majority of soundtrack albums in the dataset \\(94\\.5%\\) have clean ASCII\\-only titles, which represent the lowest\\-friction case for downstream lookups and matching\\. An additional ~4% fall into a latin\\-typographic bucket—these are still fundamentally Latin titles, but include smart quotes, accented characters, or typographic symbols, and should be safe to retain with light normalization\\. Only a small fraction of albums \\(1\\.5%\\) contain non\\-Latin scripts \\(e\\.g\\., CJK, Indic, Thai\\), which are more likely to introduce ambiguity or failure in external lookups; given their low prevalence, these can be flagged for caution or handled separately without materially affecting overall coverage\\.
""")

st.dataframe(album_df[['album_title', 'release_title']])

st.markdown("""
### Data Integrity Exploration A2\\.5: Reliability of tagging metadata for album genre
""")

st.markdown("""
Question: How comprehensive is the album tagging data? Are we able to use them as a feature? Can we standardize their values?
""")

tag_related_cols = ['release_group_mbid', 'rg_tags_text', 'rg_tags_json', 'release_tags_text',
'release_tags_json', 'label_tags_text', 'label_tags_json']
albumtags_df = album_df[tag_related_cols]

# Count the nulls and percentage nulls in the tag-related columns
null_counts = albumtags_df.isna().sum()
null_pct = (albumtags_df.isna().mean() * 100).round(2)

null_summary = (
    pd.concat([null_counts, null_pct], axis=1)
      .rename(columns={0: "null_count", 1: "null_pct"})
)

st.write(null_summary)

st.dataframe(albumtags_df.sample(50))

st.markdown("""
Finding: Album\\-level genre tags from MusicBrainz are sparsely populated \\(≈19% coverage\\) and inconsistently applied across the soundtrack corpus\\. Release\\-level tags are even more sparse, while label\\-level tags exhibit highly unpredictable and non\\-genre behavior based on visual inspection \\(e\\.g\\., organizational metadata, locations, and free\\-form noise\\)\\. As a result, genre\\-based analysis will be limited to exploratory, conditional subsets of albums where album\\-level tags are present, rather than treated as a complete or reliable feature across the full dataset\\.
""")

st.markdown("""
Let's now locate the canonical values that we can use for album\\-level genre\\.
""")

# 1. Pull out the column and keep only non-nulls
tags_series = album_df['rg_tags_text'].dropna().str.strip()

# print(len(tags_series), "\n", tags_series)

# 2. Explode the tags based on the | delimiter. Specify regex = False so | is not treated like an OR
tags_exploded = tags_series.str.split(" | ", regex = False).explode()
# print(len(tags_exploded), "\n", tags_exploded)

# 3. Count all the values
tag_counts_df = tags_exploded.value_counts().reset_index(name='count')
st.dataframe(tag_counts_df)
tag_counts_df.to_csv("rg_tags_value_counts.csv", index = False)

st.markdown("""
We investigate the community\\-specified values\\. We don't need redundant labels like "This is a soundtrack" or TV/Video game theme\\. 
""")

st.markdown("""
In a later notebook, we'll group genres into a handful of buckets and create boolean flags for them\\. For now, let's keep this notebook focused on inspecting the data
""")

st.markdown("""
# QA of the Track Table
""")

st.markdown("""
## Basic Track\\-level Checks
""")

# Read the tracks table CSV

tracks_df = pd.read_csv("./official/2.1.MUSICBRAINZ_mv_tmdb_soundtrack_track_spine_2015_2025_202601241225.csv")

st.dataframe(tracks_df.head())
st.dataframe(tracks_df.columns)

print("Rows:", len(tracks_df))
print("Unique track_ids:", tracks_df["track_id"].nunique())
print("Unique recording_ids:", tracks_df["recording_id"].nunique())

st.markdown("""
### Data Integrity Exploration T1\\.1: Describe ratios of relationships
""")

st.markdown("""
Question: Multiple tracks are associated with albums\\. Tracks can have 1 or more recording\\. Mediums \\(LP, CDs, \\.\\.\\.\\) have multiple tracks\\. What are the averages of each?
""")

# Quick ratios (helpful smell tests)
if all(c in tracks_df.columns for c in ["tmdb_id", "track_id", "recording_mbid", "medium_id"]):
    films = tracks_df["tmdb_id"].nunique(dropna=True)
    albums = tracks_df["release_group_id"].nunique(dropna=True)
    tracks = tracks_df["track_id"].nunique(dropna=True)
    recs  = tracks_df["recording_mbid"].nunique(dropna=True)
    meds  = tracks_df["medium_id"].nunique(dropna=True)
    print("\nRATIOS (unique-based)")
    print(f"  tracks_per_film (unique track_id / unique tmdb_id): {tracks/films:,.2f}")
    print(f"  tracks_per_album (unique track_id / unique release_group_id): {tracks/albums:,.2f}")
    print(f"  recordings_per_track (unique recording_mbid / unique track_id): {recs/tracks:,.3f}")
    print(f"  tracks_per_medium (unique track_id / unique medium_id): {tracks/meds:,.2f}")

st.markdown("""
Finding: In reality, we are probably only going to make use of tracks\\_per\\_film or tracks\\_per\\_album, so those are the only interesting ones we can use for estimating\\.
""")

st.markdown("""
### Data Integrity Exploration T1\\.2: Key completeness and uniqueness
""")

st.markdown("""
Question: How well populated are our different keys?
""")

# T1.2 Basic uniqueness + nulls for key IDs
KEY_IDS = ["tmdb_id", "release_group_id", "release_id", "medium_id", "track_id", "recording_mbid"]

out = []
for c in KEY_IDS:
    dup_ct = int(tracks_df.duplicated(subset = [c]).sum())
    out.append({
        "col": c,
        "n_unique": tracks_df[c].nunique(dropna=True),
        "null_ct": int(tracks_df[c].isna().sum()),
        "null_pct": float(tracks_df[c].isna().mean() * 100),
        "duplicated_rows": dup_ct,
        "duplicated_pct": float(dup_ct/len(tracks_df) * 100)
    })

pd.DataFrame(out).sort_values("col")

st.markdown("""
Findings: All IDs have been well\\-populated by our SQL scripts\\! However, there are about 4277 tracks that are duplicated \\-\\- we should investigate\\.
""")

# Isolate the duplicated tracks
dup_mask = tracks_df['track_id'].duplicated(keep = False)
dup_tracks = tracks_df[dup_mask].copy()

print("Unique duplicated track_ids:", dup_tracks['track_id'].nunique())

# Join duplicated track rows to albums_df (use release_group_id)
# keep album columns distinct
joined = dup_tracks.merge(
    album_df,
    on = ['release_group_id'],
    how="left",
    suffixes=("", "_album"),
    validate="m:m"
)

print("dup_tracks rows:", len(dup_tracks))
print("joined rows:   ", len(joined))
# auto-detected possible Altair chart: joined
try:
    st.altair_chart(joined, use_container_width=True)
except Exception:
    st.write(joined)

st.markdown("""
Findings: We need to do a track cleanup at a later stage after we do our album cleanup
""")

st.markdown("""
### Data Integrity Exploration T1\\.3: Track number analysis
""")

st.markdown("""
Question: What is the statistical characteristics of the track\\_number column?
""")

# T1.3 Track_number sanity (nulls, min/max, and weird values)

s = pd.to_numeric(tracks_df["track_number"], errors="coerce")

print("null_ct:", int(s.isna().sum()))
print("median:", float(s.median()))
print("min:", float(s.min()), "max:", float(s.max()))
print("<=0 ct:", int((s <= 0).sum()))
print(">=100 ct:", int((s >= 100).sum()))
st.write(s.describe(percentiles=[0.5, 0.9, 0.95, 0.99]))

st.markdown("""
Findings: Track numbering appears well\\-behaved across the dataset\\. All tracks have a populated track\\_number, with a median of 10 and a 95th percentile of 27, consistent with typical soundtrack album structures\\. The long tail is limited \\(99th percentile = 39\\), indicating no evidence of track\\-level join inflation or structural duplication\\. A very small number of edge cases were observed \\(3 tracks numbered 0 and 8 tracks ≥100\\), which likely reflect MusicBrainz metadata quirks or non\\-canonical releases rather than systemic issues\\. No corrective action required at this stage\\.
""")

st.markdown("""
### Data Integrity Exploration T1\\.4: Track length basic stats
""")

st.markdown("""
Question: Does the track length data look regular?
""")

# T1.4 length fields basic stats (track_length_ms + recording_length_ms)
for c in ["track_length_ms", "recording_length_ms"]:

    s = pd.to_numeric(tracks_df[c], errors="coerce")
    print("\n", c)
    print("null_ct:", int(s.isna().sum()))
    print("Median", s.median())
    print("zero_ct:", int((s == 0).sum()))
    print("neg_ct:", int((s < 0).sum()))
    st.write(s.describe(percentiles=[0.5, 0.9, 0.95, 0.99]))

st.markdown("""
Findings: Track\\- and recording\\-level duration fields exhibit strong internal consistency\\. Both track\\_length\\_ms and recording\\_length\\_ms share an identical median \\(141 seconds\\) and nearly identical upper percentiles, indicating correct alignment between track\\- and recording\\-level joins and no evidence of track duplication or inflation\\. The observed duration ranges are consistent with expected soundtrack cue lengths\\. A small number of extreme long\\-duration recordings were observed, likely reflecting full\\-score or suite\\-style releases, but these do not materially affect the overall distribution\\. 
""")

st.markdown("""
Given the high degree of overlap and consistency between the two fields, track and recording durations are effectively redundant for downstream analysis, and a single duration attribute can be selected without loss of information\\.
""")

st.markdown("""
### Data Integrity Exploration T1\\.5: Text field check
""")

st.markdown("""
Question: Which of the text fields are reliably populated?
""")

# %% [13] basic text-field empties (treat "" as missing)
TEXT_COLS = [
    "track_title",
    "recording_title",
    "recording_artist_credit",
    "composer_names_text",
    "lyricist_names_text",
    "isrcs_text",
    "recording_tags_text",
]

out = []
for c in TEXT_COLS:
    s = tracks_df[c]
    empty = s.isna() | (s.astype(str).str.strip() == "")
    out.append({
        "col": c,
        "empty_ct": int(empty.sum()),
        "empty_pct": float(empty.mean() * 100)
    })

pd.DataFrame(out).sort_values("empty_pct", ascending=False)

st.markdown("""
Findings: Text\\-based metadata fields exhibit highly uneven coverage across recordings\\. Core identification attributes such as track\\_title, recording\\_title, and recording\\_artist\\_credit are fully populated, indicating strong integrity for essential track and artist identification\\. In contrast, several enrichment fields show substantial sparsity: composer names are missing for over 70% of rows, lyricist names for over 96%, and recording tags for over 96%\\. ISRC coverage is moderate, with approximately half of recordings lacking identifiers\\. This pattern is consistent with MusicBrainz’ strength in core cataloging metadata rather than comprehensive credit or enrichment data for soundtrack recordings\\.
""")

st.markdown("""
From a previous test on album\\_df, it looks like 100% of the films have relevant composer data, so we should ignore track\\-level composer data in favor of TMDB's\\.
""")

# %% [14] quick "what values are we dealing with" for medium_format + match_method
for c in ["medium_format", "match_method"]:
    if c not in tracks_df.columns:
        print(f"{c}: missing")
        continue
    print("\n", c)
    st.dataframe(tracks_df[c].value_counts(dropna=False).head(30))

st.markdown("""
Findings: In the current pipeline, a single canonical release\\_id \\(the first one created in MusicBrainz\\) is deterministically selected per release\\_group, and only the tracks associated with that release are enumerated\\. While the chosen release may reflect a physical format \\(e\\.g\\., CD or vinyl\\) based on MusicBrainz’s internal creation order, this does not imply that a digital release does not exist for the album\\. Critically, tracklists across digital and physical releases within the same release group are typically identical at the recording level\\. As a result, medium format selection does it materially affect downstream Last\\.fm lookups, which operate on song identity rather than physical release packaging\\.
""")

st.markdown("""
## Association checks with Track
""")

st.markdown("""
### T2\\.1 Albums with extreme numbers of tracks
""")

st.markdown("""
Question: Can we identify the albums with an unusual number of tracks?
""")

# Films with extreme track counts
tpf = tracks_df.groupby("tmdb_id")["track_id"].nunique(dropna=True).rename("tracks_per_film")

top = tpf.sort_values(ascending=False).head(20).to_frame()

bottom = tpf.sort_values(ascending=True).head(20).to_frame()

bottom = bottom.join(tracks_df.groupby("tmdb_id")["film_title"].first(), how="left")

print("TOP 20")
st.write(top)

print("BOTTOM 20")
st.write(bottom)

st.markdown("""
Finding:  Track counts per film show a wide but interpretable range\\. Most films fall within an expected soundtrack size, while a small number of films exhibit unusually high track counts \\(e\\.g\\., 70–130\\+ tracks\\), likely reflecting extended scores, deluxe editions, or multi\\-disc soundtrack releases rather than structural duplication\\. At the opposite extreme, a subset of films is associated with only a single track, which is consistent with films that have a minimal soundtrack presence or only a single identified theme or song in MusicBrainz\\. These extremes are limited in number and do not indicate a systematic join inflation, but they highlight edge cases that warrant awareness in downstream analysis\\.
""")

st.markdown("""
# QA of the Wide Table
""")

# Read the wide table CSV
wide_df = pd.read_csv("./official/2.1.MUSICBRAINZ_mv_tmdb_soundtrack_wide_track_2015_2025_202601241225.csv")

st.dataframe(wide_df.columns)
st.dataframe(wide_df.head())

st.markdown("""
## Basic integrity checks
""")

st.markdown("""
### Data Integrity Exploration W1\\.1 Row count reconciliation
""")

st.markdown("""
Question: Did we construct the wide table correctly?
""")

# W1.1 shape + row-count reconciliation (wide vs album vs track)
print("wide rows:", len(wide_df))
print("wide cols:", wide_df.shape[1])

print("album rows:", len(album_df))
print("track rows:", len(tracks_df))

print("wide / tracks ratio:", (len(wide_df) / len(tracks_df)) if len(tracks_df) else np.nan)

st.markdown("""
Finding: Yes, the wide table should have a row count equal to its most granular entity, which is the track\\.
""")

st.markdown("""
### Data Integrity Exploration W1\\.2: Uniqueness of TMDB\\_ID and Track\\_ID
""")

st.markdown("""
Question: Are there duplicate tmdb\\_id and track\\_id rows?
""")

# W1.2 grain check: uniqueness at intended key (tmdb_id, track_id)
KEY = ["tmdb_id", "track_id"]

dup_rows = wide_df.duplicated(subset=KEY).sum()
print("duplicate (tmdb_id, track_id) rows:", int(dup_rows))
print("duplicate pct:", float(dup_rows / len(wide_df) * 100) if len(wide_df) else np.nan)

if dup_rows:
    display(
        wide_df.loc[wide_df.duplicated(subset=KEY, keep=False), KEY]
              .value_counts()
              .head(25)
              .rename("row_ct")
              .reset_index()
    )

# grain check: uniqueness at (release_group_id, track_id)
KEY = ["release_group_id", "track_id"]

dup_rows = wide_df.duplicated(subset=KEY).sum()
print("duplicate (release_group_id, track_id) rows:", int(dup_rows))
print("duplicate pct:", float(dup_rows / len(wide_df) * 100) if len(wide_df) else np.nan)

if dup_rows:
    display(
        wide_df.loc[wide_df.duplicated(subset=KEY, keep=False), KEY]
              .value_counts()
              .head(25)
              .rename("row_ct")
              .reset_index()
    )

st.markdown("""
Findings: This confirms that some cleanup is still required, but the issue is well\\-scoped\\. The Wide table is clean at the intended analytical grain—each \\(tmdb\\_id, track\\_id\\) pair is unique—indicating that the joins themselves did not introduce duplication\\. However, approximately 5% of rows share the same \\(release\\_group\\_id, track\\_id\\) combination\\. This reflects cases where a single soundtrack album is associated with multiple films, typically due to title collisions, remakes, or ambiguous soundtrack matching upstream\\. In other words, the same album’s tracks are being reused across films, not duplicated within a film\\. This validates the need for a post\\-matching canonicalization step at the film–album level, while also confirming that the Wide table construction is otherwise sound\\.
""")

st.markdown("""
### Data Integrity Exploration W1\\.3: 
""")

st.markdown("""
Question: Do the ids between Track and Wide match?
""")

# W1.3 join integrity: wide keys vs track spine keys (did we invent or drop rows?)
w_keys = wide_df[["tmdb_id", "track_id"]].drop_duplicates()
t_keys = tracks_df[["tmdb_id", "track_id"]].drop_duplicates()

w_only = w_keys.merge(t_keys, on=["tmdb_id", "track_id"], how="left", indicator=True).query("_merge == 'left_only'")
t_only = t_keys.merge(w_keys, on=["tmdb_id", "track_id"], how="left", indicator=True).query("_merge == 'left_only'")

print("unique (tmdb_id, track_id) in wide:", len(w_keys))
print("unique (tmdb_id, track_id) in tracks:", len(t_keys))
print("wide keys not in tracks:", len(w_only))
print("tracks keys not in wide:", len(t_only))

if len(w_only):
    print("\nSample wide-only keys:")
    st.write(w_only.head(10))

if len(t_only):
    print("\nSample tracks-only keys:")
    st.write(t_only.head(10))

st.markdown("""
Findings: The Wide table preserves the track spine exactly\\. The number of unique \\(tmdb\\_id, track\\_id\\) pairs is identical in both the track table and the Wide table, with no keys added or dropped during the join\\. This confirms that the Wide table construction did not introduce any row inflation or data loss and is a faithful projection of the underlying track spine\\.
""")
