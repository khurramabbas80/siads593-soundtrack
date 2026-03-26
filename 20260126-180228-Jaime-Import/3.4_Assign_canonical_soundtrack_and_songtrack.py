import streamlit as st
import os, sys

st.set_page_config(page_title="3.4 Assign canonical soundtrack and songtrack", layout="wide")

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

st.markdown("""
## I\\.1 Imports and DataFrame Upload
""")

# Standard library imports
import os
from datetime import datetime

# Third-party imports
import numpy as np
import pandas as pd

# os.chdir("/work")  # path adjusted for Streamlit
print(os.listdir("./pipeline"))

# Load the albums dataframe

album_df = pd.read_csv("./pipeline/3.3.Albums_deduped_df.csv")
st.dataframe(album_df.shape)
st.dataframe(album_df.columns)
st.dataframe(album_df.head())

st.markdown("""
## I\\.2 Inspect album titles
""")

st.markdown("""
Question: Are there identifying features in the album\\_title that allow us to determine which albums are orchestral "soundtracks" \\(the score of an album or a compilation of songs or something else?
""")

TITLE_PATTERNS = {
    "has_inspired_by": r"inspired by",
    "has_score": r"orchestral score|score",
    "has_songs": r"soundtrack|music from|songs from",
    "has_from_motion_picture_phrase": r"from the motion picture|from the original motion picture",
    "has_edition_marker": r"deluxe|expanded|remastered|anniversary"
}

album_titles = (
    album_df[["release_group_mbid",
    "tmdb_id",
    "album_title"]]
    .copy()
)

for k, pattern in TITLE_PATTERNS.items():
    album_titles[k] = album_titles["album_title"].str.lower().str.contains(
        pattern, regex=True
    )

# How prevalent is each designation?
inspection_summary = album_titles[list(TITLE_PATTERNS.keys())].mean().round(3)
# auto-detected possible Altair chart: inspection_summary
try:
    st.altair_chart(inspection_summary, use_container_width=True)
except Exception:
    st.write(inspection_summary)

st.markdown("""
Findings: Album titles frequently include song\\-based indicators \\(e\\.g\\., “soundtrack,” “music from”\\), appearing in roughly 40% of cases, while explicit references to scores are far less common \\(~5%\\)\\. Other variants, such as “inspired by” releases or special editions, are rare\\. This uneven use of title semantics confirms that album titles alone are insufficient for consistently identifying a single, canonical soundtrack per film, motivating the use of film\\-level rules to resolve multiple albums and support a clean, one\\-soundtrack\\-per\\-film analytic frame\\.
""")

st.markdown("""
# II\\. Determining the Canonical Soundtrack
""")

st.markdown("""
We will ceate an append table that flags the canonical soundtrack per film \\(is\\_canonical\\_soundtrack\\) and, when present, the canonical songs compilation \\(is\\_canonical\\_songtrack\\), keyed by \\(tmdb\\_id, release\\_group\\_mbid\\)\\.
""")

st.markdown("""
## II\\.1 Setup Base Tables
""")

st.markdown("""
Before applying canonical selection rules, we construct a lightweight working table that isolates the fields needed for soundtrack classification\\. This step derives simple title\\-based signals \\(e\\.g\\., “score,” “soundtrack,” excluding “inspired by”\\), counts how many albums are linked to each film, and initializes flags that will later formalize which release\\_group represents the canonical soundtrack or song compilation\\.
""")


# 1. Construct a base table for the cleanup
base_cols = ["tmdb_id", "release_group_mbid", "album_title", "match_method", "film_imdb_id"]
base_find_canon = album_df.loc[:, base_cols].copy()

# 2. Pull out the relevant title hints when present
t = base_find_canon['album_title'].str.lower()
base_find_canon['title_has_score'] = t.str.contains("score", regex = False) # includes "original score", "orchestral score", etc.
base_find_canon["title_has_soundtrack"] = (
    t.str.contains("soundtrack", regex=False)
    | t.str.contains("music from", regex=False)
    | t.str.contains("songs from", regex=False)
)

# Exclude “inspired by” from being considered canonical
base_find_canon["title_has_inspired_by"] = t.str.contains("inspired by", regex=False)

# 3. Count albums per film and add it to each record
film_album_ct = base_find_canon.groupby('tmdb_id')['release_group_mbid'].count().rename('album_ct').reset_index()
base_find_canon = base_find_canon.merge(film_album_ct, on = 'tmdb_id', how = 'left')

#4. Initialize final output flags
base_find_canon['is_canonical_soundtrack'] = False
base_find_canon['is_canonical_songtrack'] = False
base_find_canon['canonical_rule'] = ""
base_find_canon['canonical_songtrack_rule'] = ""

st.write(base_find_canon.head(10))

st.markdown("""
Findings: This setup confirms that title\\-based signals behave as expected: albums explicitly labeled “Original Motion Picture Soundtrack” are correctly flagged as soundtrack candidates, while “Original Motion Picture Score” is captured separately\\. Films with multiple albums \\(e\\.g\\., soundtrack \\+ score, or soundtrack \\+ single release\\) are clearly identifiable via album\\_ct, giving us the structure needed to programmatically select a single canonical soundtrack or song compilation in the next step\\.
""")

st.markdown("""
## II\\.2 Helper Functions for Reporting
""")


# Let's define a helper function to keep track of how many of our films are associated with exactly 1
# canonical soundtrack
def pct_films_with_exactly_one_canon(df):
    canon_ct = df.groupby("tmdb_id")["is_canonical_soundtrack"].sum()   # count of True per film
    return (canon_ct == 1).mean()

# Just in case we accidentally match more than 1 canonical soundtrack with the film.
# We are clear if the results of both functions match
def pct_films_with_any_canon(df):
    canon_any = df.groupby("tmdb_id")["is_canonical_soundtrack"].max()  # True if any row in film is True
    return canon_any.mean()

st.markdown("""
## II\\.3 Rule 1: Single album films automatically become the canonical soundtrack
""")

st.markdown("""
We start canon selection with the simplest, highest\\-confidence rule: if a film only has one album candidate in our spine, there’s nothing to resolve — that album becomes the canonical soundtrack by definition\\. This cell applies that rule and reports how much of the film set is immediately “done” before moving to trickier multi\\-album cases\\.
""")


# --------------------------------------------
# Rule 1: single-album films => canonical soundtrack
# --------------------------------------------
# If a film has exactly 1 candidate album in the spine, that album is canonical by definition.

print(f"Before Rule 1 (any canon):     {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"Before Rule 1 (exactly one):   {pct_films_with_exactly_one_canon(base_find_canon):.1%}")

# Films with exactly one album in the candidate universe
mask_single = base_find_canon["album_ct"] == 1

base_find_canon.loc[mask_single, "is_canonical_soundtrack"] = True
base_find_canon.loc[mask_single, "canonical_rule"] = "single_album_film"

print(f"After Rule 1 (any canon):      {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"After Rule 1 (exactly one):    {pct_films_with_exactly_one_canon(base_find_canon):.1%}")
print(f"Rule 1 picks applied (films):  {base_find_canon.loc[mask_single, 'tmdb_id'].nunique()}")

st.markdown("""
Findings: Rule 1 does most of the work immediately: 93\\.7% of films have only a single candidate album, allowing us to canonize 4,166 films in one pass\\. The remaining ~6% are the genuinely ambiguous multi\\-album cases that require more nuanced rules\\.
""")

st.markdown("""
## II\\.4 Rule 2: Multi\\-album films with "soundtrack" and "score" albums
""")

st.markdown("""
Next we tackle the multi\\-album films by using simple title cues\\. We flag likely score albums as canonical soundtrack candidates and likely soundtrack/music\\-from/songs\\-from albums as songtrack candidates—without forcing a one\\-per\\-film decision yet\\.
""")


# Rule 2 (multi-album title hints)
# -------------------------------
# For films with multiple albums:
# - If title indicates a "score", mark as canonical soundtrack candidate.
# - If title indicates a "soundtrack/music from/songs from", mark as songtrack candidate.
#
# NOTE: At this point, we are only flagging candidates. We have NOT enforced
# the "exactly one" constraint yet. That happens in resolve_per_film().

print(f"Before Rule 2 (any canon):     {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"Before Rule 2 (exactly one):   {pct_films_with_exactly_one_canon(base_find_canon):.1%}")

mask_multi = base_find_canon["album_ct"] > 1

# Score => canonical soundtrack
score_mask = mask_multi & base_find_canon["title_has_score"] & (~base_find_canon["title_has_inspired_by"])
base_find_canon.loc[score_mask, "is_canonical_soundtrack"] = True
base_find_canon.loc[score_mask, "canonical_rule"] = "multi_album_score_title"

# Soundtrack/music-from/songs-from => canonical songtrack
songs_mask = mask_multi & base_find_canon["title_has_soundtrack"] & (~base_find_canon["title_has_inspired_by"])
base_find_canon.loc[songs_mask, "is_canonical_songtrack"] = True
base_find_canon.loc[songs_mask, "canonical_songtrack_rule"] = "multi_album_soundtrack_title"

print(f"After Rule 2 (any canon):      {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"After Rule 2 (exactly one):    {pct_films_with_exactly_one_canon(base_find_canon):.1%}")
print(f"Rule 2 picks applied (albums): {int(score_mask.sum()):,} score flags | {int(songs_mask.sum()):,} songtrack flags")
print(f"Rule 2 picks applied (films):  {base_find_canon.loc[score_mask, 'tmdb_id'].nunique():,} score films | {base_find_canon.loc[songs_mask, 'tmdb_id'].nunique():,} songtrack films")

st.markdown("""
Findings: Rule 2 adds a meaningful bump in coverage, raising canonized films from 93\\.7% to 95\\.8%\\. It flags 96 score albums \\(95 films\\) and 219 songtrack albums \\(174 films\\), capturing many of the “soundtrack vs score” multi\\-album cases\\.
""")

st.markdown("""
## II\\.5 Rule 3: IMDB exact matches
""")

st.markdown("""
For the remaining edge cases, we apply a pragmatic fallback\\. If a film still lacks a canonical soundtrack after Rules 1–2, we select an album with an imdb\\_exact match as the canonical soundtrack\\. This leverages our highest\\-confidence linkage signal to close remaining gaps without overfitting\\.
""")


# Rule 3 (fallback: IMDB exact match)
# ----------------------------------
# If a film still has 0 canonical soundtracks after Rules 1–2, and one of its albums is an
# IMDB exact match, mark that album as the canonical soundtrack.
#
# Note: a small number of films have >1 imdb_exact match; we keep the first deterministically.

print(f"Before Rule 3 (any canon):     {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"Before Rule 3 (exactly one):   {pct_films_with_exactly_one_canon(base_find_canon):.1%}")

# Films still missing a canonical soundtrack
films_needing_rule3 = base_find_canon.groupby("tmdb_id")["is_canonical_soundtrack"].sum()
films_needing_rule3 = films_needing_rule3[films_needing_rule3 == 0].index

# Pick one imdb_exact album per film (deterministic)
rule3_picks = (
    base_find_canon[
        base_find_canon["tmdb_id"].isin(films_needing_rule3) &
        (base_find_canon["match_method"] == "imdb_exact")
    ][["tmdb_id", "release_group_mbid"]]
    .sort_values(["tmdb_id", "release_group_mbid"])
    .drop_duplicates(subset=["tmdb_id"])
)

# Apply picks using (tmdb_id, release_group_mbid) key membership
picked = base_find_canon.set_index(["tmdb_id", "release_group_mbid"]).index.isin(
    rule3_picks.set_index(["tmdb_id", "release_group_mbid"]).index
)

base_find_canon.loc[picked, "is_canonical_soundtrack"] = True
base_find_canon.loc[picked, "canonical_rule"] = "fallback_imdb_match"

print(f"After Rule 3 (any canon):      {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"After Rule 3 (exactly one):    {pct_films_with_exactly_one_canon(base_find_canon):.1%}")
print(f"Rule 3 picks applied (films):  {len(rule3_picks)}")

st.markdown("""
Findings: Rule 3 provides a final incremental lift, increasing canon coverage from 95\\.8% to 96\\.7% by resolving 42 additional films\\. At this stage, nearly the entire film set has exactly one canonical soundtrack identified, with only a small residual set remaining ambiguous\\.
""")

st.markdown("""
## II\\.6 Rule 4: Everything else: first release group becomes canonical
""")

st.markdown("""
Finally, for the small set of films still unresolved after Rules 1–3, we apply a deterministic fallback\\. Rather than introducing additional heuristics, we select a single release\\_group per film using a stable ordering rule \\(alphabetically smallest release\\_group\\_mbid\\) to guarantee complete and reproducible coverage\\.
""")


# Rule 4 (fallback: earliest release_group_mbid when still ambiguous)
# ---------------------------------------------------------------
# If a film still has 0 canonical soundtracks after Rule 3, pick a stable deterministic default:
# the earliest (alphabetically smallest) release_group_mbid within that tmdb_id.
#
# Coverage is reported the same way as Rule 3: film-level before/after using our helper functions.

print(f"Before Rule 4 (any canon):     {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"Before Rule 4 (exactly one):   {pct_films_with_exactly_one_canon(base_find_canon):.1%}")

# Films still missing a canonical soundtrack
films_still_unassigned = base_find_canon.groupby("tmdb_id")["is_canonical_soundtrack"].sum()
films_still_unassigned = films_still_unassigned[films_still_unassigned == 0].index

# Pick one album per film deterministically (earliest release_group_mbid)
rule4_picks = (
    base_find_canon[
        base_find_canon["tmdb_id"].isin(films_still_unassigned)
    ][["tmdb_id", "release_group_mbid"]]
    .sort_values(["tmdb_id", "release_group_mbid"])
    .drop_duplicates(subset=["tmdb_id"])
)

# Apply picks using (tmdb_id, release_group_mbid) key membership
picked = base_find_canon.set_index(["tmdb_id", "release_group_mbid"]).index.isin(
    rule4_picks.set_index(["tmdb_id", "release_group_mbid"]).index
)

base_find_canon.loc[picked, "is_canonical_soundtrack"] = True
base_find_canon.loc[picked, "canonical_rule"] = "fallback_earliest_mbid"

print(f"After Rule 4 (any canon):      {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"After Rule 4 (exactly one):    {pct_films_with_exactly_one_canon(base_find_canon):.1%}")
print(f"Rule 4 picks applied (films):  {len(rule4_picks)}")

st.markdown("""
Findings: Rule 4 closes the remaining gap, raising coverage from 96\\.7% to 100%\\. A total of 145 films required this deterministic fallback, ensuring that every film in the dataset now has exactly one canonical soundtrack assignment\\.
""")

st.markdown("""
As a final guardrail, we run a safety pass to enforce the one\\-film, one\\-canonical\\-soundtrack constraint\\. If any film somehow has multiple canonical flags after the rule cascade, we retain a single deterministic winner and unflag the rest\\.
""")

# ------------------------------------------------------------
# Safety pass (simple): if a film has >1 canon flagged, keep ONE
# ------------------------------------------------------------

print(f"Before safety pass (any canon):     {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"Before safety pass (exactly one):   {pct_films_with_exactly_one_canon(base_find_canon):.1%}")

# Only look at rows currently flagged as canonical soundtrack
canon_rows = base_find_canon[base_find_canon["is_canonical_soundtrack"]].copy()

# For each tmdb_id, keep the first canonical row; everything else gets unflagged
keep_idx = canon_rows.groupby("tmdb_id", sort=False).head(1).index
drop_idx = canon_rows.index.difference(keep_idx)

base_find_canon.loc[drop_idx, "is_canonical_soundtrack"] = False
base_find_canon.loc[drop_idx, "canonical_rule"] = "safety_pass_dropped"

# (Optional) tag the winners so we can tell later
base_find_canon.loc[keep_idx, "canonical_rule"] = (
    base_find_canon.loc[keep_idx, "canonical_rule"].fillna("safety_pass_winner")
)

print(f"Safety pass: dropped extra canon rows: {len(drop_idx)}")

print(f"After safety pass (any canon):      {pct_films_with_any_canon(base_find_canon):.1%}")
print(f"After safety pass (exactly one):    {pct_films_with_exactly_one_canon(base_find_canon):.1%}")

# Quick peek at what got dropped
display(
    base_find_canon.loc[drop_idx, ["tmdb_id", "release_group_mbid", "album_title", "match_method", "canonical_rule"]]
    .head(25)
)

st.markdown("""
Findings: The safety pass removed just one extra canonical row, confirming that the rule cascade was already well\\-behaved\\. Coverage remains at 100%, with exactly one canonical soundtrack per film\\.
""")

st.markdown("""
# III\\. Create the append table and validate
""")

st.markdown("""
## III1\\. Append Table
""")

st.markdown("""
With the canonical selection logic finalized, we materialize a clean film–album append table at the \\(tmdb\\_id, release\\_group\\_mbid\\) grain\\. This table carries the canonical flags and rule provenance, allowing us to verify key uniqueness before merging the results back onto album\\_df and locking in a structurally sound, one\\-canonical\\-per\\-film layer\\.
""")

# Append table + quick checks
# ---------------------------
# Now that we’ve applied Rules 1–4, we can materialize the append table at film–album grain.
# Primary key: (tmdb_id, release_group_mbid)
# Requirement: exactly 1 canonical soundtrack per film (tmdb_id)

soundtrack_append = base_find_canon.loc[:, [
    "tmdb_id",
    "release_group_mbid",
    "album_title",
    "album_ct",
    "is_canonical_soundtrack",
    "is_canonical_songtrack",
    "canonical_rule",
    "canonical_songtrack_rule"
]].copy()

st.write(soundtrack_append.head(25))

st.markdown("""
There really shouldn't be any dupes across the composite key of \\(tmdb\\_id, release\\_group\\_mbid\\),

but just in case \\-\\- let's validate\\.
""")

album_key_dupes = album_df.duplicated(["tmdb_id", "release_group_mbid"]).sum()
print("album_df duplicate (tmdb_id, release_group_mbid) keys:", album_key_dupes)

append_key_dupes = soundtrack_append.duplicated(["tmdb_id", "release_group_mbid"]).sum()
print("soundtrack_append duplicate keys:", append_key_dupes)

st.markdown("""
Confirmed that there are no dupes indeed\\! So we are ready to merge\\.\\.\\.
""")

# Merge canonical flags back onto album_df (no need to rebuild soundtrack_append if it already exists)
album_df_before = len(album_df)

album_mrg_df = album_df.merge(
    soundtrack_append[[
        "tmdb_id",
        "release_group_mbid",
        "is_canonical_soundtrack",
        "is_canonical_songtrack",
        "canonical_rule",
        "canonical_songtrack_rule"
    ]].drop_duplicates(["tmdb_id", "release_group_mbid"]),
    on=["tmdb_id", "release_group_mbid"],
    how="left",
    validate="1:1"   # safe even if album_df happens to be 1:1
)

print("album_df rows before:", album_df_before)
print("album_df rows after: ", len(album_mrg_df))

st.markdown("""
The merge preserves row integrity: album\\_df remains at 4,771 rows before and after the canonical flag append, confirming a clean 1:1 merge at the \\(tmdb\\_id, release\\_group\\_mbid\\) level with no duplication or unintended row expansion\\.
""")

st.markdown("""
## III\\.2 Validation
""")


# -----------------------------------------
# Compare canonical soundtrack count vs films
# -----------------------------------------

# Number of canonical soundtracks flagged (row-level)
canon_soundtrack_ct = album_mrg_df["is_canonical_soundtrack"].sum()

# Number of unique films
film_ct = album_mrg_df["tmdb_id"].nunique()

print(f"Canonical soundtracks flagged (rows): {int(canon_soundtrack_ct):,}")
print(f"Unique films (tmdb_id):                {film_ct:,}")
print(f"Difference (canon - films):            {int(canon_soundtrack_ct - film_ct):,}")

st.markdown("""
Hmm, there is still one straggler
""")

# -----------------------------------------
# Find films with >1 canonical soundtrack
# -----------------------------------------

canon_per_film = album_mrg_df.groupby("tmdb_id")["is_canonical_soundtrack"].sum()

bad_tmdb_ids = canon_per_film[canon_per_film > 1].index.tolist()
print("Films with >1 canonical soundtrack:", len(bad_tmdb_ids))
print("Bad tmdb_ids:", bad_tmdb_ids[:20])

bad_rows = album_mrg_df.loc[
    album_mrg_df["tmdb_id"].isin(bad_tmdb_ids) & (album_mrg_df["is_canonical_soundtrack"]),
    ["tmdb_id", "film_title", "release_group_mbid", "album_title", "match_method", "canonical_rule"]
].sort_values(["tmdb_id", "canonical_rule", "release_group_mbid"])

st.write(bad_rows)

# PK uniqueness
print("PK dupes:", soundtrack_append.duplicated(["tmdb_id", "release_group_mbid"]).sum())

base_find_canon["canonical_rule"].value_counts(dropna=False)

canon_per_film = base_find_canon.groupby("tmdb_id")["is_canonical_soundtrack"].sum()
canon_per_film.value_counts().sort_index()

# Visually inspect the fallback matches
display(base_find_canon[base_find_canon["canonical_rule"].str.contains("fallback", na=False)]
        .loc[:, ["tmdb_id","album_title","is_canonical_soundtrack","is_canonical_songtrack","canonical_rule"]
        ].sort_values('tmdb_id')
        )

st.markdown("""
A quick spot check confirms that canonical selections are largely intuitive and rule\\-driven, with most resolved via fallback\\_imdb\\_match or the deterministic fallback\\. A few albums are flagged as both canonical soundtrack and songtrack \\(e\\.g\\., traditional OSTs\\), which is expected for releases that function as both score and compilation\\. Overall, the outputs look structurally consistent with our rule hierarchy rather than arbitrary assignments\\.
""")

st.markdown("""
## III\\.3 Songtrack exploration \\(not as important\\)
""")

song_per_film = base_find_canon.groupby("tmdb_id")["is_canonical_songtrack"].sum()
song_per_film.value_counts().sort_index()

# Songtrack sanity check (Might be more than 1 per film)
song_check = soundtrack_append.groupby("tmdb_id")["is_canonical_songtrack"].sum()
song_bad = song_check[song_check > 1]
print("Films with songtrack > 1:", len(song_bad))
st.write(song_bad.head(20))

st.markdown("""
# IV\\. Two more extra calculations: films and album age
""")

st.markdown("""
The film release dates and album release dates have been useful for matching film and soundtrack, but they are also useful as a potential feature for statistical analysis\\. However, in their current form, probably not so much\\. They would be more useful as age columns\\. Let's compute the days\\_since\\_film\\_release and days\\_since\\_album\\_release columns
""")

st.markdown("""
### IV\\.1 Quick inspection
""")

# Unfortunately, some of the release dates may be missing elements. Let's
# create a function to inspect missingness around a specific column

def inspect_date_missingness(df:pd.DataFrame, col:str):
    # 1) Raw missing (NaN / None / NaT)
    raw_missing_ct = df[col].isna().sum()
    raw_missing_pct = raw_missing_ct / len(album_mrg_df)

    # 2) Missing after parsing (catches blanks, bad strings, invalid dates)
    parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
    parsed_missing_ct = parsed.isna().sum()
    parsed_missing_pct = parsed_missing_ct / len(album_mrg_df)

    # 3) (Optional) Treat empty/whitespace strings as missing explicitly (for visibility)
    empty_str_mask = df[col].astype("string").str.strip().eq("")
    empty_str_ct = empty_str_mask.sum()
    empty_str_pct = empty_str_ct / len(album_mrg_df)

    print(f"Total rows: {len(album_mrg_df):,}")
    print(f"Raw missing ({col} isna): {raw_missing_ct:,} ({raw_missing_pct:.2%})")
    print(f"Empty/blank strings:       {empty_str_ct:,} ({empty_str_pct:.2%})")
    print(f"Missing after to_datetime: {parsed_missing_ct:,} ({parsed_missing_pct:.2%})")

    # 4) Show a few problematic examples (non-missing raw, but unparseable)
    bad_examples = df.loc[~album_mrg_df[col].isna() & parsed.isna(), col].value_counts(dropna=False).head(20)
    print("\nTop unparseable raw values (up to 20):")
    st.write(bad_examples)

inspect_date_missingness(album_mrg_df, "film_release_date")

st.markdown("""
Findings: film\\_release\\_date is fully populated and clean\\. There are no raw nulls, blank strings, or parsing failures, indicating 100% completeness and no malformed date values in the merged album layer\\.
""")

# Some dates may be incomplete. Let's inspect whether a date column has
# year, month and day

def inspect_date_completeness(df:pd.DataFrame, col):
    raw = df[col].astype("string")
    s = raw.str.strip()

    parsed = pd.to_datetime(s, errors="coerce", utc=True)

    # Start with default label
    gran = pd.Series("unparseable", index=album_mrg_df.index, dtype="string")

    # Missing / blank
    gran.loc[s.isna() | s.eq("")] = "missing_blank"

    # Strict formats
    gran.loc[s.str.fullmatch(r"\d{4}-\d{2}-\d{2}", na=False)] = "yyyy-mm-dd"
    gran.loc[s.str.fullmatch(r"\d{4}-\d{2}", na=False)]       = "yyyy-mm"
    gran.loc[s.str.fullmatch(r"\d{4}", na=False)]             = "yyyy"

    # Parseable but not matching strict formats (e.g., "Mar 3 2020")
    gran.loc[parsed.notna() & ~s.isna() & ~s.eq("") &
            ~s.str.fullmatch(r"\d{4}(-\d{2}(-\d{2})?)?", na=False)] = "other_parseable"

    # Summarize
    summary = gran.value_counts(dropna=False).to_frame("count")
    summary["pct"] = summary["count"] / len(df)

    st.write(summary)

inspect_date_completeness(album_mrg_df, col = "film_release_date")

st.markdown("""
Findings: Film release date is fully populated, and every film listed has complete information in the format yyyy\\-mm\\-dd\\. Converting it to 'days\\_since\\_film\\_release' will be clean\\.
""")

st.markdown("""
Let's now inspect 'album\\_us\\_release\\_date'
""")

inspect_date_missingness(album_mrg_df, "album_us_release_date")

inspect_date_completeness(album_mrg_df, "album_us_release_date")

st.markdown("""
Findings: It appears that 'album\\_us\\_release\\_date' is missing from a lot of records \\-\\- only ~30% of records have them at this stage in the pipeline\\. The good news is that 100% of the 1424 remaining records all have fully populated 'yyyy\\-mm\\-dd' values\\. We might still be able to use a 'days\\_since\\_album\\_age' attribute as a feature in our analysis, but it won't be that reliable\\.
""")

asof = pd.Timestamp.now(tz="UTC")   # Safeguard if notebook gets run in different timezones
album_mrg_df['days_since_film_release'] = (asof - pd.to_datetime(album_mrg_df['film_release_date'], errors='coerce', utc = True)).dt.days
st.dataframe(album_mrg_df[['tmdb_id', 'film_title', 'film_release_date', 'days_since_film_release']].sample(10))

album_mrg_df['days_since_album_release'] = (asof - pd.to_datetime(album_mrg_df['album_us_release_date'], errors='coerce', utc = True)).dt.days
st.dataframe(album_mrg_df[['release_group_id', 'album_title','album_us_release_date','days_since_album_release']].sample(10))

st.markdown("""
# V\\. Augment and export base tables
""")

st.markdown("""
### V\\.1 Setup Artist, Tracks and Wide
""")

import shutil

shutil.copy(
    "./pipeline/3.3.Artists_deduped_df.csv",
    "./pipeline/3.4.Artists_canonical_identified_df.csv"
)

shutil.copy(
    "./pipeline/3.3.Tracks_deduped_df.csv",
    "./pipeline/3.4.Tracks_canonical_identified_df.csv"
)


st.dataframe(album_df.head())

album_mrg_df.to_csv(
     "./pipeline/3.4.Albums_canonical_identified_df.csv",
     index=False
)

st.markdown("""
### V\\. 2 Wide Table augmentation with canonical flag
""")

st.markdown("""
We really need to augment the wide table as well
""")

# Load the wide dataframe

wide_df = pd.read_csv("./pipeline/3.3.Wide_deduped_df.csv")
st.dataframe(wide_df.shape)
st.dataframe(wide_df.columns)
st.dataframe(wide_df.head())

wide_before = len(wide_df)

wide_mrg_df = wide_df.merge(
    soundtrack_append[[
        "tmdb_id",
        "release_group_mbid",
        "is_canonical_soundtrack",
        "is_canonical_songtrack",
        "canonical_rule",
        "canonical_songtrack_rule"
    ]].drop_duplicates(["tmdb_id", "release_group_mbid"]),
    on=["tmdb_id", "release_group_mbid"],
    how="left",
    validate="m:1"   # many track rows per (tmdb_id, release_group_mbid)
)

print("wide_df rows before:", wide_before)
print("wide_df rows after: ", len(wide_mrg_df))

# 1) Coverage: how many rows got canon flags populated at all?
print("Rows with is_canonical_soundtrack not-null:",
      wide_mrg_df["is_canonical_soundtrack"].notna().sum())

# 2) Sanity: canonical soundtrack rows should exist (unless you haven't flagged yet)
print("Rows flagged canonical soundtrack (track rows):",
      (wide_mrg_df["is_canonical_soundtrack"] == True).sum())

st.markdown("""
Finding: It looks a little shocking at first that most rows in the wide table are marked as coming from a canonical soundtrack\\. In practice, this is mostly a side effect of the data structure\\. Over 93% of films in the dataset only map to a single soundtrack album, so those albums are unambiguous and are marked canonical by definition\\. When those albums are exploded out to the track level, each one contributes many rows, which makes the canonical flag look dominant in the wide table\\. The skew is coming from track\\-level expansion, not from aggressive or subjective classification\\.
""")

# Album-grain check inside wide_df:
#    Each film should map to exactly 1 canonical soundtrack album after resolution.
canon_album_ct_by_film = (
    wide_mrg_df.loc[wide_mrg_df["is_canonical_soundtrack"] == True]
      .groupby("tmdb_id")["release_group_mbid"]
      .nunique()
)

print("\nFilms w/ >=1 canonical soundtrack album (in wide_df):", (canon_album_ct_by_film >= 1).sum())
print("Films w/ exactly 1 canonical soundtrack album:", (canon_album_ct_by_film == 1).sum())
print("Films w/ >1 canonical soundtrack album:", (canon_album_ct_by_film > 1).sum())



st.markdown("""
Finding: All films that appear in the wide table resolve cleanly to exactly one
 canonical soundtrack album\\. This confirms that the canonicalization logic held up when expanded to the track level, with no remaining multi\\-album
 ambiguity after the final safety pass\\.

""")

st.markdown("""
### V\\.3 Calculate film and album ages for wide
""")

before = len(wide_mrg_df.columns)
wide_mrg_df['days_since_film_release'] = (asof - pd.to_datetime(wide_mrg_df['film_release_date'], errors='coerce', utc = True)).dt.days
wide_mrg_df['days_since_album_release'] = (asof - pd.to_datetime(wide_mrg_df['album_us_release_date'], errors='coerce', utc = True)).dt.days
after = len(wide_mrg_df.columns)
print(f"Before wide_df column count: {before} and after: {after}")

# Quick inspection

st.dataframe(wide_mrg_df[['tmdb_id', 'film_title', 'film_release_date', 'days_since_film_release']].sample(10))
st.dataframe(wide_mrg_df[['release_group_id', 'album_title','album_us_release_date','days_since_album_release']].sample(10))

st.markdown("""
### V\\. 4 Export wide table
""")

st.dataframe(wide_mrg_df.head())

wide_mrg_df.to_csv(
     "./pipeline/3.4.Wide_canonical_identified_df.csv",
     index=False
)
