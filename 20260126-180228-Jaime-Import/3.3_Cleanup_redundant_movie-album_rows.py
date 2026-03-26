import streamlit as st
import os, sys

st.set_page_config(page_title="3.3 Cleanup redundant movie-album rows", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# I\\. Setup and Album Title Inspection
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

album_df = pd.read_csv("./pipeline/3.2.Albums_official_df.csv")
st.dataframe(album_df.shape)
st.dataframe(album_df.columns)
st.dataframe(album_df.head())


st.markdown("""
# II\\. Duplicate Album Exploration
""")

st.markdown("""
Question: Do we have albums mapped to more than one film? That would be clearly wrong\\. How do we ensure that albums only map to a single film?
""")

# List out the albums that show up on multiple films
rg_multi_films = (
    album_df.groupby("release_group_id")["tmdb_id"].nunique()
)
rg_multi_films = rg_multi_films[rg_multi_films > 1]
st.write(rg_multi_films)

st.write(f"There are {rg_multi_films.sum()} dupe records spread out across {rg_multi_films.shape[0]} albums")
# Finding: there are (form. 219) 197 albums that are associated with more than 1 film.
# We need to go through (form. 500) 448 records to see what to delete

st.markdown("""
Findings: 197 albums map to multiple films, producing 448 ambiguous links that require manual disambiguation or removal
""")

# Extract the release_group_ids from the Series index
multi_rg_mbids = rg_multi_films.index
dupe_cols = ['release_group_id', 'match_method', 'album_title', 'album_us_release_date', 'film_title', 'film_imdb_id', 'film_release_date', 'film_soundtrack_composer_raw', 'film_vote_count', 'tmdb_id']

# Create the dataframe with all the contextual columns to help us cleanup
rg_dupe_list = album_df.loc[album_df['release_group_id'].isin(multi_rg_mbids), dupe_cols]
st.write(rg_dupe_list)

# Export to .csv (for eyeballing) and identify the rgs for deletion
rg_dupe_list.to_csv("release_groups_linked_to_multiple_films.csv", index=False)

st.markdown("""
# III\\. Dupe Removal
""")

st.markdown("""
## III\\.1 Setup and inspection
""")

st.markdown("""
We begin by flagging all rows as retained and then isolate release\\_groups associated with multiple films\\. For these “known dupes,” we construct a focused review dataset containing key linkage variables \\(match method, titles, release dates, composer, vote count, etc\\.\\)\\. We also initialize a removal tracker so that any disambiguation decisions made here can be consistently applied later to downstream datasets \\(e\\.g\\., wide\\_df\\)\\.
""")

# 1) Keep flag: start by keeping everything
album_df["keep_row"] = True

# 2) Build the dupe list
multi_rg_mbids = rg_multi_films.index

dupe_cols = [
    "release_group_id",
    "match_method",
    "album_title",
    "album_us_release_date",
    "film_title",
    "film_imdb_id",
    "film_release_date",
    "film_soundtrack_composer_raw",
    "film_vote_count",
    "tmdb_id",
]

rg_dupe_list = album_df.loc[
    album_df["release_group_id"].isin(multi_rg_mbids),
    dupe_cols
].copy()

# 3) Denominator set: which release_groups are "known dupes" that need resolving
dupe_rg_set = set(rg_dupe_list["release_group_id"].dropna().unique())

# 4) We'll track what we remove so we can apply the same deletions to wide_df later
# Each entry is a tuple: (release_group_id, tmdb_id)
removed_links = []

# ------------------------------------------------------------
# Baseline sanity check + quick peek at the dupes
# ------------------------------------------------------------

# Quick look: which release groups map to how many films (in current kept set)
kept_links = album_df.loc[album_df["keep_row"], ["release_group_id", "tmdb_id"]].dropna()
rg_to_tmdb_ct = (
    kept_links[kept_links["release_group_id"].isin(dupe_rg_set)]
    .groupby("release_group_id")["tmdb_id"]
    .nunique()
    .sort_values(ascending=False)
)

st.write(rg_to_tmdb_ct.head(25))

st.markdown("""
Findings: Several release\\_groups map to as many as six distinct films, confirming that duplicate linkages are not isolated edge cases but concentrated structural ambiguities requiring explicit resolution\\.
""")

st.markdown("""
## III\\.2 Helper Functions
""")

st.markdown("""
We use a couple lightweight helper functions to keep the duplicate\\-resolution workflow disciplined and repeatable\\. dupe\\_progress\\(\\) gives us a quick snapshot of how many multi\\-film release\\_groups we’ve fully resolved \\(now mapping to a single TMDB film\\) versus how many still remain\\. Separately, \\_norm\\_title\\(\\) and titles\\_seem\\_related\\(\\) provide a conservative “name match” heuristic we use in Pass 1 to flag obvious album–film mismatches without over\\-deleting ambiguous cases\\.
""")

# Progress tracker: how far we've gotten resolving multi-film release_group duplicates
def dupe_progress():
    # Only consider links we've decided to keep so far (drop any incomplete keys)
    kept_links = album_df.loc[album_df["keep_row"], ["release_group_id", "tmdb_id"]].dropna()

    # For the known-dupe release_groups, count how many distinct films each is still linked to
    # (After cleanup, a "resolved" release_group should map to exactly 1 tmdb_id.)
    rg_to_tmdb_ct = (
        kept_links[kept_links["release_group_id"].isin(dupe_rg_set)]
        .groupby("release_group_id")["tmdb_id"]
        .nunique()
    )

    # % resolved: share of dupe release_groups that now map to exactly one film
    pct_resolved = (rg_to_tmdb_ct == 1).mean()

    # % present: share of dupe release_groups that still appear at all in the kept set
    # (If a release_group disappears, we likely removed all its rows.)
    pct_present  = pd.Series(list(dupe_rg_set)).isin(set(kept_links["release_group_id"])).mean()

    return pct_resolved, pct_present

# ------------------------------------------------------------
# Light text normalization + a "name match" check
#
# Purpose:
#   Pass 1 uses a simple heuristic to catch obvious mismatches where an album
#   title and film title are clearly unrelated (e.g., a generic album title
#   incorrectly linked to a different film with the same name).
#
# Guardrail:
#   This is intentionally conservative. If we can't confidently evaluate a case,
#   we return True (i.e., "seems related") so we don't delete based on this rule alone.
# ------------------------------------------------------------

def _norm_title(s):
    """
    Normalize titles into a comparable form:
      - lowercase
      - remove punctuation/symbols
      - collapse repeated whitespace
    This makes token overlap checks more reliable across messy real-world strings.
    """
    if pd.isna(s):
        return ""
    s = str(s).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)   # replace punctuation with spaces
    s = re.sub(r"\s+", " ", s).strip()   # collapse whitespace
    return s

def titles_seem_related(album_title, film_title):
    """
    Heuristic match between an album title and a film title.

    Why we need this:
      Album titles often include boilerplate ("Original Motion Picture Soundtrack"),
      which can obscure the actual identifying words. We strip common soundtrack
      phrases, then look for simple signals that the remaining words relate to
      the film title.

    Returns:
      True  -> titles plausibly relate (keep / don't delete on this rule)
      False -> titles look unrelated (candidate for deletion in Pass 1)

    Rules (in priority order):
      1) If we can't evaluate (empty after cleaning), return True (conservative).
      2) If one normalized title contains the other, return True (strong signal).
      3) Otherwise, require at least 2 shared meaningful tokens (len >= 3).
    """
    a = _norm_title(album_title)
    f = _norm_title(film_title)

    # Remove boilerplate soundtrack phrases from the album title so we can compare
    # the "core" identifying words (e.g., "Dune" vs "Original Motion Picture Soundtrack").
    boilerplate = [
        "original motion picture soundtrack",
        "motion picture soundtrack",
        "original soundtrack",
        "soundtrack",
        "original score",
        "score",
        "music from the motion picture",
        "music from the film",
        "ost",
    ]
    for b in boilerplate:
        a = a.replace(_norm_title(b), " ")

    a = re.sub(r"\s+", " ", a).strip()

    # Conservative default: if we don't have enough signal, we avoid deleting
    if not a or not f:
        return True

    # Strong signal: containment after normalization
    if a in f or f in a:
        return True

    # Softer signal: token overlap (ignore tiny words like "of", "the", etc.)
    a_tokens = {t for t in a.split() if len(t) >= 3}
    f_tokens = {t for t in f.split() if len(t) >= 3}
    return len(a_tokens & f_tokens) >= 2

st.markdown("""
## III\\.3 Pass 1: Eliminate name mismatches between film and album
""")

st.markdown("""
Pass 1: fast, conservative cleanup\\. For duplicate\\-linked release\\_groups, we drop rows where the album title clearly doesn’t match the film title, and log those removals for reuse downstream\\.
""")

# ------------------------------------------------------------
# PASS 1 — Title mismatch filter (conservative)
# Goal: For release_groups linked to multiple films, remove the most obvious
# bad links where the album title and film title clearly don't match.
#
# Design choices:
# - Only operate on rows still marked keep_row=True
# - Only operate within the "known dupes" set (dupe_rg_set)
# - Use a conservative heuristic: if we can't evaluate, we do NOT delete
# - Log deletions as (release_group_id, tmdb_id) so we can replay them later (wide_df, tracks, etc.)
# ------------------------------------------------------------

# Scope: only rows we're still keeping, and only release_groups known to be duplicated across films
mask_in_scope = (
    album_df["keep_row"]
    & album_df["release_group_id"].isin(dupe_rg_set)
)

# For in-scope rows, evaluate whether album_title and film_title seem related
# (titles_seem_related() returns True when the titles plausibly match)
name_related = album_df.loc[mask_in_scope].apply(
    lambda r: titles_seem_related(r["album_title"], r["film_title"]),
    axis=1
)

# Initialize a full-length drop mask (aligned to album_df), then mark in-scope rows
# as drop=True when titles do NOT seem related.
mask_drop = pd.Series(False, index=album_df.index)
mask_drop.loc[album_df.loc[mask_in_scope].index] = ~name_related.values

# Record removed links for downstream deletions:
# Each entry is (release_group_id, tmdb_id). We store links (not full rows)
# so the same removals can be applied to other tables later.
removed = album_df.loc[mask_drop, ["release_group_id", "tmdb_id"]].dropna()
removed_links.extend(list(map(tuple, removed.values)))

# Apply the decision: mark these rows as not kept
album_df.loc[mask_drop, "keep_row"] = False

# Quick progress report: how many dupes now resolve to a single film, and how many are still present at all
pct_resolved, pct_present = dupe_progress()
print(
    f"After Pass 1 (name mismatch): dropped={mask_drop.sum():,} | "
    f"pct_resolved={pct_resolved:.2%} | pct_present={pct_present:.2%}"
)

st.markdown("""
There was only one film with a clear film title to album name mismatch \\(The Boss \\-\\-\\> The Boss Baby\\) \\-\\- so our SQL extraction did a pretty good job keeping this clean\\.
""")

st.markdown("""
## III\\.4 Pass 2: Favor imdb\\_matches
""")

st.markdown("""
Pass 2: prefer higher\\-confidence links\\. For duplicate\\-linked release\\_groups, if we have at least one imdb\\_exact match, we treat that as authoritative and drop any weaker title\\_contains\\_strict rows within the same release\\_group\\.
""")

# ------------------------------------------------------------
# PASS 2 — Prefer imdb_exact within each duplicated release_group
# Rule: If a release_group has any imdb_exact match, keep those links and
#       drop title_contains_strict links for that same release_group.
# ------------------------------------------------------------

# Work only within the current kept set and only for release_groups that are known dupes
kept_scope = album_df.loc[
    album_df["keep_row"] & album_df["release_group_id"].isin(dupe_rg_set),
    ["release_group_id", "match_method"]
].copy()

# Identify release_groups where we have at least one high-confidence imdb_exact link
rgs_with_imdb_exact = set(
    kept_scope.loc[kept_scope["match_method"] == "imdb_exact", "release_group_id"].unique()
)

# Drop the weaker title_contains_strict links, but only when an imdb_exact exists for that release_group
mask_drop = (
    album_df["keep_row"]
    & album_df["release_group_id"].isin(rgs_with_imdb_exact)
    & (album_df["match_method"] == "title_contains_strict")
)

# Log removed (release_group_id, tmdb_id) pairs so we can replay deletions downstream
removed = album_df.loc[mask_drop, ["release_group_id", "tmdb_id"]].dropna()
removed_links.extend(list(map(tuple, removed.values)))

# Apply the decision in-place
album_df.loc[mask_drop, "keep_row"] = False

# Report progress after this pass
pct_resolved, pct_present = dupe_progress()
print(
    f"After Pass 2 (prefer imdb_exact): dropped={mask_drop.sum():,} | "
    f"pct_resolved={pct_resolved:.2%} | pct_present={pct_present:.2%}"
)

st.markdown("""
## III\\.5 Pass 3: Favor films with higher vote counts
""")

st.markdown("""
Pass 3: break ties using film exposure\\. When a release\\_group still maps to multiple films after the higher\\-confidence rules, we keep the link\\(s\\) to the film with the highest TMDB vote count \\(ties allowed\\) and drop the lower\\-exposure candidates\\.
""")

# ------------------------------------------------------------
# PASS 3 — Prefer the film with the highest vote_count within each duplicated release_group
# Rationale: vote_count is our best lightweight proxy for film exposure/visibility.
# Rule: keep the row(s) tied for max vote_count; drop only rows that are strictly lower.
# ------------------------------------------------------------

# Scope: only rows still kept, and only for release_groups that are known dupes
scope = album_df["keep_row"] & album_df["release_group_id"].isin(dupe_rg_set)

# Pull only what we need and coerce vote_count to numeric for safe comparisons
tmp = album_df.loc[scope, ["release_group_id", "film_vote_count"]].copy()
tmp["film_vote_count"] = pd.to_numeric(tmp["film_vote_count"], errors="coerce").fillna(0)

# For each release_group, compute the maximum vote_count among the currently kept rows
# (transform keeps the result aligned row-by-row with tmp)
max_vote_by_rg = tmp.groupby("release_group_id")["film_vote_count"].transform("max")

# Mark drops: any row with a strictly lower vote_count than the release_group's max
mask_drop = scope.copy()
mask_drop.loc[scope] = tmp["film_vote_count"].values < max_vote_by_rg.values

# Log removed (release_group_id, tmdb_id) pairs for replay downstream
removed = album_df.loc[mask_drop, ["release_group_id", "tmdb_id"]].dropna()
removed_links.extend(list(map(tuple, removed.values)))

# Apply the decision in-place
album_df.loc[mask_drop, "keep_row"] = False

# Report progress after this pass
pct_resolved, pct_present = dupe_progress()
print(
    f"After Pass 3 (vote_count): dropped={mask_drop.sum():,} | "
    f"pct_resolved={pct_resolved:.2%} | pct_present={pct_present:.2%}"
)

st.markdown("""
### III\\.6 Remove film\\_album dupes
""")

st.markdown("""
Pass 4: clean up TMDB\\-side title collisions\\. Even after picking the best film link within each release\\_group, we can still end up with duplicate mappings driven by generic film titles \\(e\\.g\\., multiple films named the same thing\\)\\. In this pass, we dedupe at the \\(film\\_title, album\\_title\\) level and keep only one release\\_group association per title pair, prioritizing higher\\-confidence matches and higher\\-exposure films\\.
""")

# ------------------------------------------------------------
# PASS 4 — TMDB-side title collisions: dedupe (film_title, album_title)
#
# Problem this targets:
#   Some films have common titles ("Limbo", "Inside", etc.). That can produce multiple
#   film–album mappings that look identical by name, even if they point to different
#   release_groups / tmdb_ids.
#
# Rule:
#   For each (film_title, album_title) pair, keep exactly one association and drop the rest.
#   Preference order:
#     1) imdb_exact matches over title_contains_strict
#     2) higher film_vote_count (proxy for exposure)
#     3) stable tie-breaker: lowest release_group_id
# ------------------------------------------------------------

# Scope: only rows still kept, and only for release_groups in our "known dupes" set
scope = album_df["keep_row"] & album_df["release_group_id"].isin(dupe_rg_set)

# Pull the minimum columns needed to rank and dedupe
tmp = album_df.loc[scope, [
    "film_title", "album_title", "release_group_id", "tmdb_id",
    "match_method", "film_vote_count"
]].copy()

# Ensure vote_count is numeric so sorting behaves as expected
tmp["film_vote_count"] = pd.to_numeric(tmp["film_vote_count"], errors="coerce").fillna(0)

# Binary flag so we can sort imdb_exact ahead of other match methods
tmp["is_imdb_exact"] = (tmp["match_method"] == "imdb_exact").astype(int)

# Rank within each (film_title, album_title) group using our preference order
tmp = tmp.sort_values(
    ["film_title", "album_title", "is_imdb_exact", "film_vote_count", "release_group_id"],
    ascending=[True, True, False, False, True]
)

# Keep the single best row per (film_title, album_title); drop the rest
keepers = tmp.drop_duplicates(subset=["film_title", "album_title"], keep="first")[
    ["film_title", "album_title", "release_group_id", "tmdb_id"]
]

# Build a set of keeper keys so we can mark all non-keepers for deletion
keep_set = set(zip(
    keepers["film_title"], keepers["album_title"],
    keepers["release_group_id"], keepers["tmdb_id"]
))

# Build row-level keys for all candidate rows in tmp
all_rows = tmp[["film_title", "album_title", "release_group_id", "tmdb_id"]]
row_keys = list(zip(
    all_rows["film_title"], all_rows["album_title"],
    all_rows["release_group_id"], all_rows["tmdb_id"]
))

# Local drop mask: anything not in the keep_set gets dropped
mask_drop_local = ~pd.Series(row_keys).isin(keep_set).values

# Expand the local mask back to album_df index space
mask_drop = pd.Series(False, index=album_df.index)
mask_drop.loc[tmp.index] = mask_drop_local

# Log removed links for replay downstream
removed = album_df.loc[mask_drop, ["release_group_id", "tmdb_id"]].dropna()
removed_links.extend(list(map(tuple, removed.values)))

# Apply the decision in-place
album_df.loc[mask_drop, "keep_row"] = False

# Report progress after this pass
pct_resolved, pct_present = dupe_progress()
print(
    f"After Pass 4 (film_title+album_title dedupe): dropped={mask_drop.sum():,} | "
    f"pct_resolved={pct_resolved:.2%} | pct_present={pct_present:.2%}"
)

st.markdown("""
By wiping out some of the duplicate films, we ended up wiping out some of the release groups as well\\. Let's take a look at this list of release groups eliminated\\.\\.\\.
""")

wiped_rgs = sorted(list(dupe_rg_set - set(
    album_df.loc[album_df["keep_row"], "release_group_id"].unique()
)))

album_df.loc[
    album_df["release_group_id"].isin(wiped_rgs),
    ["release_group_id", "release_group_id", "album_title", "film_title", "film_vote_count", "tmdb_id"]
]

st.markdown("""
Final structural deduplication review\\.
The release groups removed in this last pass were mostly tied to one\\-word film titles \\(e\\.g\\., Drive, Alone, Gold, Limbo\\) — exactly the kinds of names that create ambiguous matches across multiple TMDB entries\\. In nearly all cases, we retained the higher\\-confidence or higher\\-exposure mapping and dropped redundant ones\\. The one higher\\-vote outlier \\(The Little Mermaid\\) reflects multiple adaptations sharing the same title rather than a flaw in the rules\\. Overall, this step cleaned up title\\-level ambiguity, not legitimate soundtrack links\\.
""")

st.markdown("""
## III\\.7 Generate the delete list
""")

st.markdown("""
With the duplicate\\-resolution passes complete, we now consolidate every deletion decision into a single, canonical remove list\\. This converts our row\\-level “keep/drop” actions into unique \\(release\\_group\\_id, tmdb\\_id\\) pairs that can be replayed consistently across downstream tables \\(especially track\\_df and wide\\_df\\), ensuring the entire pipeline stays in sync\\.
""")

# ------------------------------------------------------------
# Final delete list (unique pairs) to apply to track_df and wide_df later
# ------------------------------------------------------------

removed_links_df = (
    pd.DataFrame(removed_links, columns=["release_group_id", "tmdb_id"])
      .dropna()
      .drop_duplicates()
      .sort_values(["release_group_id", "tmdb_id"])
      .reset_index(drop=True)
)

print(f"Delete pairs captured: {len(removed_links_df):,}")
st.dataframe(removed_links_df.head(50))

pct_resolved, pct_present = dupe_progress()
print(f"Final progress: pct_resolved={pct_resolved:.2%} | pct_present={pct_present:.2%}")

st.markdown("""
Summary: The duplicate\\-resolution passes had very different impacts\\. Pass 1 \\(title mismatch\\) didn’t remove anything, suggesting that obvious name errors were not driving the ambiguity\\. Pass 2 \\(prefer imdb\\_exact\\) immediately resolved the vast majority of duplicate release groups without dropping rows, indicating that higher\\-confidence matches were already present\\. Pass 3 \\(vote count\\) did the heavy lifting, removing 194 lower\\-exposure links and resolving nearly all remaining conflicts\\. Pass 4 \\(film–album title dedupe\\) cleaned up the final edge cases, bringing resolution to 100%\\. In total, 265 ambiguous film–album links were removed through this structured process\\.
""")

st.markdown("""
## III\\.7 Final validation
""")

album_df.loc[album_df["keep_row"]] \
.groupby("release_group_id")["tmdb_id"] \
.nunique() \
.value_counts()


st.markdown("""
Every album \\(release group\\) is now associated with one and only one film \\(tmdb\\_id\\)\\. Nice job\\!
""")

# Without keep_row filter (raw)
film_to_rg_ct_all = (
    album_df
    .groupby("tmdb_id")["release_group_id"]
    .nunique()
)

print("Without keep_row filter (raw)")
st.write(film_to_rg_ct_all.value_counts().sort_index())

# With keep_row filter applied
film_to_rg_ct_kept = (
    album_df.loc[album_df["keep_row"]]
    .groupby("tmdb_id")["release_group_id"]
    .nunique()
)

print("With keep_row == True")
st.write(film_to_rg_ct_kept.value_counts().sort_index())



st.markdown("""
Films can be associated with more than 1 album, so don't be surprised that there are still 2s, 3s \\.\\.\\. 5s\\. The counts have reduced all around though\\. Good\\.
""")

# Delete list integrity list

# Should be unique pairs
removed_links_df.duplicated().sum()

# Should not overlap with kept rows
overlap = set(zip(
    removed_links_df["release_group_id"],
    removed_links_df["tmdb_id"]
)) & set(zip(
    album_df.loc[album_df["keep_row"], "release_group_id"],
    album_df.loc[album_df["keep_row"], "tmdb_id"]
))

len(overlap)

st.markdown("""
Confirmed that the delete list has no overlap with the "keep" rows
""")

st.markdown("""
# IV\\. Deletions and exports
""")

st.markdown("""
### IV\\.1 Deletes from album\\_df
""")

st.markdown("""
Keep only the rows in album\\_df where 'keep\\_row' is 1\\.
""")

# -----------------------------------------
# Delete rows marked keep_row == False
# -----------------------------------------

rows_before = len(album_df)

albums_deduped_df = album_df.loc[album_df["keep_row"]].copy()

rows_after = len(albums_deduped_df)

print(f"Rows before deduping: {rows_before:,}")
print(f"Rows after deduping:  {rows_after:,}")
print(f"Rows removed:        {rows_before - rows_after:,}")

# -----------------------------------------
# Export deduped album dataframe
# -----------------------------------------

albums_deduped_df.to_csv(
    "./pipeline/3.3.Albums_deduped_df.csv",
    index=False
)

st.markdown("""
### IV\\.2 Deletes from track\\_df
""")

tracks_df = pd.read_csv("./pipeline/3.2.Tracks_official_df.csv")
st.dataframe(tracks_df.columns)
st.dataframe(tracks_df.head())

# -----------------------------------------
# Delete rows from tracks_df using composite key
# (release_group_id, tmdb_id)
# -----------------------------------------

rows_before = len(tracks_df)

# Build a set of pairs to delete
delete_pairs = set(
    zip(
        removed_links_df["release_group_id"],
        removed_links_df["tmdb_id"]
    )
)

# Build the composite key for tracks_df
tracks_pairs = list(
    zip(
        tracks_df["release_group_id"],
        tracks_df["tmdb_id"]
    )
)

# Keep rows NOT in the delete list
mask_keep = [pair not in delete_pairs for pair in tracks_pairs]

tracks_df_deduped = tracks_df.loc[mask_keep].copy()

rows_after = len(tracks_df_deduped)

print(f"tracks_df rows before: {rows_before:,}")
print(f"tracks_df rows after:  {rows_after:,}")
print(f"tracks_df rows removed:{rows_before - rows_after:,}")

tracks_df_deduped.to_csv(
    "./pipeline/3.3.Tracks_deduped_df.csv",
    index=False
)

st.markdown("""
Delete the corresponding rows from tracks\\_df
""")

# -----------------------------------------
# Delete rows from tracks_df using composite key
# (release_group_id, tmdb_id)
# -----------------------------------------

rows_before = len(tracks_df)

delete_pairs = set(zip(
    removed_links_df["release_group_id"],
    removed_links_df["tmdb_id"]
))

tracks_pairs = list(zip(
    tracks_df["release_group_id"],
    tracks_df["tmdb_id"]
))

mask_keep = [pair not in delete_pairs for pair in tracks_pairs]

tracks_df_deduped = tracks_df.loc[mask_keep].copy()

rows_after = len(tracks_df_deduped)

print(f"tracks_df rows before: {rows_before:,}")
print(f"tracks_df rows after:  {rows_after:,}")
print(f"tracks_df rows removed:{rows_before - rows_after:,}")

# Quick sanity: confirm none of the delete pairs remain
remaining = set(zip(tracks_df_deduped["release_group_id"], tracks_df_deduped["tmdb_id"])) & delete_pairs
print("delete pairs still present:", len(remaining))

tracks_df_deduped.to_csv(
    "./pipeline/3.3.Tracks_deduped_df.csv",
    index=False
)

# Confirm the “problem signature”: dupes at (release_group_id, track_id)
dup_rg_track = tracks_df_deduped.duplicated(subset=["release_group_id", "track_id"], keep=False)

print("dup (release_group_id, track_id) rows:", int(dup_rg_track.sum()))
print("dup pct:", float(dup_rg_track.mean() * 100))

# how many unique dup pairs?
print("unique dup pairs:", tracks_df_deduped.loc[dup_rg_track, ["release_group_id","track_id"]].drop_duplicates().shape[0])

st.markdown("""
Very nice\\! The cleanup has eliminated the duplicative records that was causing redundant \\(release\\_group\\_id, track\\_id\\)\\!
""")

st.markdown("""
### IV\\.3 Deletes from wide\\_df
""")

# For completeness, though we don't delete any rows from Artists or Tracks, we should push it
# down the pipeline
import shutil

shutil.copy(
    "./pipeline/3.2.Artists_official_df.csv",
    "./pipeline/3.3.Artists_deduped_df.csv"
)

wide_df = pd.read_csv("./pipeline/3.2.Wide_official_df.csv")
st.dataframe(wide_df.head())

# -----------------------------------------
# Delete rows from wide_df using composite key
# (release_group_id, tmdb_id)
# -----------------------------------------

rows_before = len(wide_df)

delete_pairs = set(zip(
    removed_links_df["release_group_id"],
    removed_links_df["tmdb_id"]
))

wide_pairs = list(zip(
    wide_df["release_group_id"],
    wide_df["tmdb_id"]
))

mask_keep = [pair not in delete_pairs for pair in wide_pairs]

wide_df_deduped = wide_df.loc[mask_keep].copy()

rows_after = len(wide_df_deduped)

print(f"wide_df rows before: {rows_before:,}")
print(f"wide_df rows after:  {rows_after:,}")
print(f"wide_df rows removed:{rows_before - rows_after:,}")

# Quick sanity: confirm none of the delete pairs remain
remaining = set(zip(wide_df_deduped["release_group_id"], wide_df_deduped["tmdb_id"])) & delete_pairs
print("delete pairs still present:", len(remaining))

wide_df_deduped.to_csv(
    "./pipeline/3.3.Wide_deduped_df.csv",
    index=False
)

st.markdown("""
Bonus finding: Our cleanup also removed all the \\(release\\_group, track\\_id duplicates\\!\\)
""")

# Confirm the “problem signature”: dupes at (release_group_id, track_id)
dup_rg_track = wide_df_deduped.duplicated(subset=["release_group_id", "track_id"], keep=False)

print("dup (release_group_id, track_id) rows:", int(dup_rg_track.sum()))
print("dup pct:", float(dup_rg_track.mean() * 100))

# how many unique dup pairs?
print("unique dup pairs:", wide_df_deduped.loc[dup_rg_track, ["release_group_id","track_id"]].drop_duplicates().shape[0])
