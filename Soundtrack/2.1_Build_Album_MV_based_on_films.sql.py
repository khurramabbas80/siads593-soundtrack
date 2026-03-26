import streamlit as st
import os, sys

st.set_page_config(page_title="2.1 Build_Album_MV_based_on_films.sql", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# Documentation\\-Only SQL Artifact
""")

st.markdown("""
This notebook contains a verbatim copy of the SQL script \\`Build\\_Album\\_MV\\_based\\_on\\_films\\.sql\\` for transparency and review\\.
""")

st.markdown("""
It is provided for reproducibility documentation only and should not be executed within the notebook runtime\\.
""")

st.markdown("""
Materialized view creation must occur in the canonical Postgres database environment to preserve schema integrity and QA controls then exported in a CSV, which is then read in by notebooks 3\\.1 and 3\\.2
""")

st.markdown("""
### CELL 1 — TMDB staging table \\(DDL\\)
""")

st.markdown("""
Defines the staging table used to ingest the TMDB 2015–2025 export into Postgres\\. This establishes the canonical schema for film metadata prior to any soundtrack matching
""")

/* ============================================================================
TMDB Movies (2015–2025) → MusicBrainz Postgres (staging load)
-------------------------------------------------------------------------------
# auto-detected possible Altair chart: Purpose
try:
    st.altair_chart(Purpose, use_container_width=True)
except Exception:
    st.write(Purpose)
- Safely ingest a large TMDB export into the `musicbrainz` schema without
  touching core MusicBrainz tables.
- Use a staging table so you can reload/drop/filter freely and build joins later.

What “good” looks like
- COPY finishes successfully
- Row count in Postgres matches the CSV
- Spot checks show titles/dates/ids in the right columns (no comma/quote drift)
============================================================================ */

/*****************************************
 * PART 1. Loading the TMDB Staging Table
 ******************************************/

/* ---------------------------------------------------------------------------
Step 1.1 — Create a staging table (matches TMDB CSV headers exactly)
# auto-detected possible Altair chart: Why
try:
    st.altair_chart(Why, use_container_width=True)
except Exception:
    st.write(Why)
- Keeps external TMDB data isolated from MusicBrainz canonical data.
- Supports repeatable reloads and downstream transformations/joins.
# auto-detected possible Altair chart: Notes
try:
    st.altair_chart(Notes, use_container_width=True)
except Exception:
    st.write(Notes)
- Column names intentionally mirror the CSV header (including spaces/punctuation) so GUI imports and positional COPY work cleanly.
- Quoted column names preserve spaces/punctuation and must match the CSV header exactly.
- Use TEXT for list-like fields and anything that may exceed small VARCHAR limits (cast/crew/studios/keywords/genres).
--------------------------------------------------------------------------- */
/* Staging table aligned to DBeaver-generated INSERT column names.
Key point: unquoted identifiers become lowercase in Postgres; quoted ones are case-sensitive. */

-- OPTIONAL RESET: drops downstream MV; not required for staging load.
-- Note that this can only be dropped if the Materialized Views from 2.2 are dropped first
-- DESTRUCTIVE: drops/recreates staging table (safe; this is raw TMDB data only).
DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025;

DROP TABLE IF EXISTS musicbrainz.tmdb_movies_2015_2025_staging;

CREATE TABLE musicbrainz.tmdb_movies_2015_2025_staging (
  id BIGINT,
  title TEXT,
  adult BOOLEAN,
  "Runtime (min)" INTEGER,
  genres TEXT,
  "Rating (0-10)" REAL,
  "Vote Count" INTEGER,
  "MPAA Rating" TEXT,
  "Original Title" TEXT,
  "Language Name" TEXT,
  "IMDb ID" TEXT,
  "Wikidata ID" TEXT,
  countries TEXT,
  "year" INTEGER,
  "Release Date" TEXT,
  popularity NUMERIC,
  budget BIGINT,
  revenue BIGINT,
  studios TEXT,
  director TEXT,
  "Soundtrack/Composer" TEXT,
  "Top Cast" TEXT,
  keywords TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

COMMIT;

st.markdown("""
### CELL 2 — Post\\-load validation queries
""")

st.markdown("""
Validation checks confirming correct ingestion of the TMDB dataset: row counts, ID integrity, date ranges, and spot checks for structural consistency\\.
""")

/* ---------------------------------------------------------------------------
Step 1.2 — Load TMDB CSV into staging table (DBeaver import)
# auto-detected possible Altair chart: Why
try:
    st.altair_chart(Why, use_container_width=True)
except Exception:
    st.write(Why)
- Ingest the TMDB 2015–2025 export into Postgres without touching core
  MusicBrainz tables.
- Keep the raw TMDB data isolated so it can be reloaded, validated, or
  discarded independently of downstream logic.

# auto-detected possible Altair chart: How
try:
    st.altair_chart(How, use_container_width=True)
except Exception:
    st.write(How)
- Use DBeaver’s “Import Data” → CSV workflow targeting:
    musicbrainz.tmdb_movies_2015_2025_staging
- Map columns by name (not position).
- CSV header must exactly match the staging table column names
  (including spaces and punctuation).

# auto-detected possible Altair chart: Notes
try:
    st.altair_chart(Notes, use_container_width=True)
except Exception:
    st.write(Notes)
- Leave "Release Date" as TEXT; date parsing is handled downstream.
- Empty strings may be imported as NULL.
- Re-running the import requires truncating or recreating the staging table.
- If re-importing the same CSV, you can TRUNCATE the staging table first (faster than DROP/CREATE).

# auto-detected possible Altair chart: Next
try:
    st.altair_chart(Next, use_container_width=True)
except Exception:
    st.write(Next)
- Run the post-load validation queries in Step 3 to confirm a clean ingest.
--------------------------------------------------------------------------- */


/* Post-load validation
Goal: confirm the TMDB CSV loaded cleanly (row counts, key coverage, basic sanity checks).
Why: catches common ingestion issues like partial loads, shifted columns, empty key fields, or unexpected date ranges. */

-- 1) Total row count
SELECT COUNT(*) AS row_count
FROM musicbrainz.tmdb_movies_2015_2025_staging;
-- 31,971 rows

-- 2) ID coverage + uniqueness
-- tmdb_id_nonnull: how many rows have an ID present (should be close to total rows).
-- tmdb_id_distinct: how many unique IDs exist (should be close to nonnull unless duplicates exist).
-- tmdb_id_nulls: indicates missing IDs (ideally 0).
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  COUNT(id) AS tmdb_id_nonnull,
  COUNT(DISTINCT id) AS tmdb_id_distinct,
  COUNT(*) - COUNT(id) AS tmdb_id_nulls
FROM musicbrainz.tmdb_movies_2015_2025_staging;
-- tmdb non-null  	distinct	nulls
-- 31971			31971		0

-- 3) Spot check sample rows
-- Quick “eyes on” validation that key fields look sane and aligned (no column drift).
-- If drift happened, there will be obvious nonsense (e.g., vote counts in the title column).
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  id,
  title,
  "Release Date",
  "Vote Count",
  "IMDb ID"
FROM musicbrainz.tmdb_movies_2015_2025_staging
ORDER BY id NULLS LAST
LIMIT 25;
-- Table looks sane

-- 4) Release date range sanity check
-- Uses MIN/MAX to confirm you’re seeing roughly 2015–2025 and not crazy outliers.
-- Note: Release Date is TEXT in your staging table; lexical MIN/MAX is fine for YYYY-MM-DD strings.
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  MIN("Release Date") AS min_release_date,
  MAX("Release Date") AS max_release_date
FROM musicbrainz.tmdb_movies_2015_2025_staging;
-- Confirmed: 2015-01-01 to 2025-12-31

-- 5) Blank title check
-- Titles should almost never be blank; blanks often signal parse/mapping problems or bad source records.
SELECT COUNT(*) AS blank_titles
FROM musicbrainz.tmdb_movies_2015_2025_staging
WHERE title IS NULL OR btrim(title) = '';
-- 1 blank title

st.markdown("""
### CELL 3 — Final matches table \\+ IMDb\\-exact logic
""")

st.markdown("""
Defines the authoritative match table and populates high\\-confidence soundtrack matches where TMDB IMDb IDs align directly with MusicBrainz soundtrack release groups\\.
""")


/*******************************************
 * Part 2. Building the IMDB_id match table
 *******************************************/


/* ============================================================================
Part 2.1 Creating the match table

OBJECT: musicbrainz.tmdb_mb_soundtrack_matches_2015_2025
PURPOSE:
  Persist TMDB → MusicBrainz soundtrack-related matches in a single, auditable
  table. This table captures all valid relationships between films and their
  soundtrack-related MusicBrainz release groups (e.g., original score,
  original soundtrack, singles, inspired-by albums), enabling:
    (a) transparent inspection of one-to-many film ↔ soundtrack relationships,
    (b) phased application of matching strategies (IMDb, Wikidata, title-based),
    (c) downstream analytical filtering and aggregation without irreversible
        assumptions baked into the data model.
GRAIN:
  One row per (tmdb_id, release_group_id). A single film may legitimately map
  to multiple soundtrack-related release groups.
COLUMNS:
  - tmdb_id:
      TMDB film identifier.
  - release_group_id:
      MusicBrainz release_group identifier for a soundtrack-related album.
  - match_method:
      How the match was obtained (e.g., imdb_exact, wikidata_exact,
      title_year_strict, title_year_fuzzy).
  - match_score:
      Numeric confidence score for the match. Exact identifier matches
      typically use 1.0; heuristic or fuzzy matches may use lower values.
  - matched_at:
      Timestamp indicating when the match record was created.
  - is_score / is_soundtrack / is_single:
      Rough boolean classifiers derived from release group naming patterns.
      These are heuristics for analysis convenience and may be refined later.
  - soundtrack_type:
      Coarse mutually exclusive label intended for filtering/aggregation in
      notebooks. Initial values: (score, songs, inspired_by, single, unknown).
PRIMARY KEY:
  (tmdb_id, release_group_id)
NOTES:
  - This table intentionally does NOT enforce a single “canonical” soundtrack
    per film. Canonical selection is deferred to the analysis layer.
  - Classification fields are best-effort heuristics and should be treated as
    approximate until refined/validated.
============================================================================ */
DROP TABLE IF EXISTS musicbrainz.tmdb_mb_soundtrack_matches_2015_2025;

CREATE TABLE musicbrainz.tmdb_mb_soundtrack_matches_2015_2025 (
  tmdb_id BIGINT NOT NULL,
  release_group_id INTEGER NOT NULL,
  match_method TEXT NOT NULL,
  soundtrack_type TEXT NOT NULL,
  matched_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (tmdb_id, release_group_id)
);

COMMIT;

/* ============================================================================
Part 2.2 Populate IMDb-exact matches (TMDB → MusicBrainz Soundtrack Release Groups)
-------------------------------------------------------------------------------
GOAL:
  Insert high-confidence matches where a TMDB film’s IMDb ID exactly matches an
  IMDb title URL attached to at least one MusicBrainz release_group whose
  secondary type is Soundtrack.
MATCH DEFINITION:
  - TMDB imdb_id = canonical tt-id extracted from musicbrainz.url for a
    soundtrack release_group.
GRAIN:
  One row per (tmdb_id, release_group_id). A film may map to multiple
  release groups (score vs soundtrack vs singles).
CLASSIFICATION (heuristic, best-effort):
  - soundtrack_type in (inspired_by, score, songs, single, unknown)
SCOPE:
  - Uses only TMDB rows with a nonblank "IMDb ID".
  - Uses only MB release_groups with secondary_type = 'Soundtrack' and an IMDb tt-id URL.
RERUN SAFETY:
  - ON CONFLICT prevents duplicate inserts.
============================================================================ */
TRUNCATE TABLE musicbrainz.tmdb_mb_soundtrack_matches_2015_2025;

# auto-detected possible Altair chart: WITH
try:
    st.altair_chart(WITH, use_container_width=True)
except Exception:
    st.write(WITH)
/* CTE: rg_imdb_tt
   Build release_group_id -> IMDb tt_id by scanning URL links attached to release groups.
   Extract the tt####### token from the URL and keep only rows that contain one. */
rg_imdb_tt AS (
  SELECT DISTINCT
    lru.entity0 AS release_group_id,
    -- Extract the IMDb tt_id from the URL.
	-- Example: https://www.imdb.com/title/tt1234567/ → tt1234567
    regexp_replace(u.url, '.*(tt[0-9]+).*', '\1') AS tt_id
  FROM musicbrainz.l_release_group_url lru
  JOIN musicbrainz.url u
    ON u.id = lru.entity1
  WHERE u.url ~ 'tt[0-9]+'
),
/* CTE: soundtrack_rg
   Identify release groups explicitly labeled with secondary type “Soundtrack”.
   This avoids relying on title text alone to decide what’s a soundtrack. */
soundtrack_rg AS (
  SELECT DISTINCT
    rgstj.release_group AS release_group_id
  FROM musicbrainz.release_group_secondary_type_join rgstj
  JOIN musicbrainz.release_group_secondary_type rgst
    ON rgst.id = rgstj.secondary_type
  WHERE lower(rgst.name) = 'soundtrack'
),
/* CTE: rg_imdb_soundtrack
   Keep only soundtrack release groups that also have an IMDb tt_id link.
   This becomes the matchable set for joining to TMDB by IMDb ID. */
rg_imdb_soundtrack AS (
  SELECT
    r.release_group_id,
    r.tt_id
  FROM rg_imdb_tt r
  JOIN soundtrack_rg s
    ON s.release_group_id = r.release_group_id
)
/* Insert
   Match TMDB films to MB soundtrack release groups by exact IMDb tt_id equality.
   Normalize TMDB IMDb IDs (trim, blank->NULL), classify soundtrack_type from rg.name, and upsert safely. */
INSERT INTO musicbrainz.tmdb_mb_soundtrack_matches_2015_2025
  (tmdb_id, release_group_id, match_method, soundtrack_type)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  t.id AS tmdb_id,
  rg.id AS release_group_id,
  'imdb_exact' AS match_method,
  /* Heuristic label based on release_group.name (quick bucketing for QA/EDA). */
  CASE
    WHEN lower(rg.name) LIKE '%inspired by%' THEN 'inspired_by'
    WHEN lower(rg.name) LIKE '%score%'
      OR lower(rg.name) LIKE '%orchestral score%' THEN 'score'
    WHEN lower(rg.name) LIKE '%soundtrack%'
      OR lower(rg.name) LIKE '%music from%'
      OR lower(rg.name) LIKE '%songs from%' THEN 'songs'
    WHEN (lower(rg.name) LIKE '%from the motion picture%'
       OR lower(rg.name) LIKE '%from the original motion picture%')
      AND lower(rg.name) NOT LIKE '%soundtrack%'
      AND lower(rg.name) NOT LIKE '%score%' THEN 'single'
    ELSE 'unknown'
  END AS soundtrack_type
FROM musicbrainz.tmdb_movies_2015_2025_staging t
JOIN rg_imdb_soundtrack m
  /* Clean IMDb ID (trim + blank->NULL) so the join behaves predictably. */
  ON m.tt_id = NULLIF(btrim(t."IMDb ID"), '')
JOIN musicbrainz.release_group rg
  ON rg.id = m.release_group_id
WHERE NULLIF(btrim(t."IMDb ID"), '') IS NOT NULL
ON CONFLICT (tmdb_id, release_group_id) DO NOTHING;

COMMIT;

st.markdown("""
### CELL 4 — Coverage reporting \\(IMDb\\-exact baseline\\)
""")

st.markdown("""
Computes vote\\-count bucket coverage to quantify baseline soundtrack match rates across films of varying popularity\\.
""")


select * from musicbrainz.tmdb_mb_soundtrack_matches_2015_2025;

select count(*) from musicbrainz.tmdb_mb_soundtrack_matches_2015_2025
-- 1526

/* ============================================================================
Step 2.3 Validate IMDb-exact match coverage
Goal: Measure what % of TMDB films (2015–2025 pull) have at least one persisted
      soundtrack match in tmdb_mb_soundtrack_matches_2015_2025, by vote-count bucket.
Matched definition: a film is “matched” if it has ≥1 row in the matches table.
============================================================================ */

/* ---------------------------------------------------------------------------
2.3 COMMAND 1: Bucket TMDB films by vote_count (TEMP table)
Why: Creates a simple, inspectable “one row per film” table with:
  - vote_count_range: human-readable bucket label
  - vote_count_sort: stable numeric order for reporting
Notes:
  - TEMP tables live only for this session/connection.
  - COALESCE("Vote Count", 0) treats NULL as 0 so every film lands in a bucket.
--------------------------------------------------------------------------- */

DROP TABLE IF EXISTS tmdb_movies_bucketed;

CREATE TEMP TABLE tmdb_movies_bucketed AS
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  id AS tmdb_id,
  COALESCE("Vote Count", 0) AS vote_count, -- Normalize NULL vote counts to 0 so every film can be bucketed.
  CASE -- vote_count_range: bucket label used for grouping/display.
    WHEN COALESCE("Vote Count", 0) BETWEEN 0 AND 100 THEN '0-100'
    WHEN COALESCE("Vote Count", 0) BETWEEN 101 AND 250 THEN '101-250'
    WHEN COALESCE("Vote Count", 0) BETWEEN 251 AND 500 THEN '251-500'
    WHEN COALESCE("Vote Count", 0) BETWEEN 501 AND 750 THEN '501-750'
    WHEN COALESCE("Vote Count", 0) BETWEEN 751 AND 1000 THEN '751-1000'
    WHEN COALESCE("Vote Count", 0) BETWEEN 1001 AND 1500 THEN '1001-1500'
    WHEN COALESCE("Vote Count", 0) BETWEEN 1501 AND 2000 THEN '1501-2000'
    WHEN COALESCE("Vote Count", 0) BETWEEN 2001 AND 3000 THEN '2001-3000'
    WHEN COALESCE("Vote Count", 0) BETWEEN 3001 AND 5000 THEN '3001-5000'
    WHEN COALESCE("Vote Count", 0) BETWEEN 5001 AND 7500 THEN '5001-7500'
    WHEN COALESCE("Vote Count", 0) BETWEEN 7501 AND 10000 THEN '7501-10000'
    ELSE '10001+'
  END AS vote_count_range,
  CASE -- vote_count_sort: numeric sort key so buckets order logically (not alphabetically).
    WHEN COALESCE("Vote Count", 0) BETWEEN 0 AND 100 THEN 1
    WHEN COALESCE("Vote Count", 0) BETWEEN 101 AND 250 THEN 2
    WHEN COALESCE("Vote Count", 0) BETWEEN 251 AND 500 THEN 3
    WHEN COALESCE("Vote Count", 0) BETWEEN 501 AND 750 THEN 4
    WHEN COALESCE("Vote Count", 0) BETWEEN 751 AND 1000 THEN 5
    WHEN COALESCE("Vote Count", 0) BETWEEN 1001 AND 1500 THEN 6
    WHEN COALESCE("Vote Count", 0) BETWEEN 1501 AND 2000 THEN 7
    WHEN COALESCE("Vote Count", 0) BETWEEN 2001 AND 3000 THEN 8
    WHEN COALESCE("Vote Count", 0) BETWEEN 3001 AND 5000 THEN 9
    WHEN COALESCE("Vote Count", 0) BETWEEN 5001 AND 7500 THEN 10
    WHEN COALESCE("Vote Count", 0) BETWEEN 7501 AND 10000 THEN 11
    ELSE 12
  END AS vote_count_sort
FROM musicbrainz.tmdb_movies_2015_2025_staging;

/* ---------------------------------------------------------------------------
2.3 COMMAND 2: Aggregate match rates by vote_count bucket (no extra temp tables)
Why: Reports how often films in each vote_count bucket have ≥1 soundtrack match row.
Notes:
  - EXISTS prevents double-counting when one film maps to many release_group rows.
  - LEFT JOIN is not needed here because we start from the full bucketed film set.
--------------------------------------------------------------------------- */
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  b.vote_count_range,
  COUNT(*) AS films, -- Total films in this vote_count bucket.
  SUM(CASE WHEN EXISTS (SELECT 1 FROM musicbrainz.tmdb_mb_soundtrack_matches_2015_2025 m WHERE m.tmdb_id = b.tmdb_id)
           THEN 1 ELSE 0 END) AS matched_films, -- Films with at least one match row.
  ROUND(
    100.0 * SUM(CASE WHEN EXISTS (SELECT 1 FROM musicbrainz.tmdb_mb_soundtrack_matches_2015_2025 m WHERE m.tmdb_id = b.tmdb_id)
                     THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0),
    2
  ) AS match_rate_pct -- Percent of films in the bucket that are matched.
FROM tmdb_movies_bucketed b
GROUP BY b.vote_count_range, b.vote_count_sort
ORDER BY b.vote_count_sort;



/*
 *  Range		Films	Match	%
 * 	10-100		24088	205		0.85
 *	101-250		3589	157		4.37
 *	251-500		1699	164		9.65
 *	501-750		671		116		17.29
 *	751-1000	367		68		18.53
 *	1001-1500	443		110		24.83
 *	1501-2000	248		75		30.24
 *	2001-3000	284		132		46.48
 *	3001-5000	252		140		55.56
 *	5001-7500	140		98		70.00
 *	7501-10000	77		61		79.22
 *	10001+		113		97		85.84
 */





st.markdown("""
### CELL 5 — Candidate infrastructure \\+ normalization tables
""")

st.markdown("""
Creates the heuristic candidate table and normalized film/album title tables to support scalable title\\-containment matching logic\\.
""")

/*********************************
*   Part 3. Heuristic Matching
*********************************/

/* ============================================================================
HEURISTIC TITLE MATCHING — TITLE CONTAINS (simple, explainable baseline)

PURPOSE:
  Measure incremental match-rate lift by matching TMDB films that failed IMDb
  exact matching to MusicBrainz soundtrack release groups using a simple
  case-insensitive substring check:
    lower(release_group.name) LIKE '%' || lower(tmdb.title) || '%'

WHY THIS STRATEGY:
  - Soundtrack album titles almost always contain the film title.
  - Much easier to reason about than trigram similarity.
  - Fast, transparent, and sufficient for measuring incremental lift.

SCOPE CONTROLS:
  - Only TMDB films with NO imdb_exact match.
  - Only MusicBrainz release groups with secondary_type = 'Soundtrack'.
  - Year constraint: ±1 year between TMDB release year and MB US release year.

OUTPUT:
  Inserts rows into tmdb_mb_soundtrack_matches_2015_2025
  with match_method = 'title_contains'.
============================================================================ */

/* ============================================================================
OBJECT: musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025
-------------------------------------------------------------------------------
PURPOSE:
  Persist NON-FINAL, heuristic TMDB → MusicBrainz soundtrack candidate links
  (e.g., title containment), separate from the final match table.

WHY THIS EXISTS:
  - Keeps musicbrainz.tmdb_mb_soundtrack_matches_2015_2025 “clean” and suitable
    for downstream joins and exports without fuzzy noise.
  - Enables a review workflow: candidates can be inspected, filtered, scored,
    and selectively promoted into the final matches table.
  - Allows analytics to measure incremental lift from heuristics without
    baking those assumptions into the canonical mapping layer.

GRAIN:
  One row per (tmdb_id, release_group_id, match_method).

PRIMARY KEY:
  (tmdb_id, release_group_id, match_method)

NOTES:
  - Unlike the final matches table (which has one row per tmdb_id × release_group_id),
    candidates preserve the match_method in the key to allow multiple heuristic
    strategies to propose the same mapping without collisions.
  - `notes` is a lightweight human-readable explanation of the heuristic; it is
    not intended to be machine-parsed.
============================================================================ */
DROP TABLE IF EXISTS musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025;

CREATE TABLE musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 (
  tmdb_id BIGINT NOT NULL,
  release_group_id INTEGER NOT NULL,
  match_method TEXT NOT NULL,
  match_score NUMERIC,
  matched_at TIMESTAMPTZ DEFAULT now(),
  soundtrack_type TEXT,
  is_score BOOLEAN,
  is_soundtrack BOOLEAN,
  is_single BOOLEAN,
  notes TEXT,
  PRIMARY KEY (tmdb_id, release_group_id, match_method)
);

CREATE INDEX tmdb_mb_soundtrack_candidates_2015_2025_tmdb_id_idx
  ON musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 (tmdb_id);

CREATE INDEX tmdb_mb_soundtrack_candidates_2015_2025_rg_id_idx
  ON musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 (release_group_id);

COMMIT;


/* ============================================================================
STEP 3.1: Build a clean TMDB film title normalization table (disqualify 1-letter)
-------------------------------------------------------------------------------
GOAL:
  Precompute normalized film titles once (avoid repeated regexp_replace inside
  large joins), and immediately disqualify titles that cannot produce reliable
  fuzzy candidates.
DISQUALIFICATION RULES:
  - Exclude titles whose normalized character count (ignoring spaces) <= 1.
OUTPUT:
  musicbrainz.tmdb_film_norm_2015_2025
GRAIN:
  One row per tmdb_id.
COLUMNS:
  - tmdb_id
  - film_title_raw
  - film_norm (lowercase, alnum-only, spaces)
  - film_char_ct (chars excluding spaces)
  - film_token_ct (# tokens split on whitespace)
============================================================================ */

DROP TABLE IF EXISTS musicbrainz.tmdb_film_norm_2015_2025;

CREATE TABLE musicbrainz.tmdb_film_norm_2015_2025 AS
WITH base AS (
  SELECT
    t.id AS tmdb_id,
    t.title AS film_title_raw,
    /* Normalize title:
       Example: "  Spider-Man: No Way Home  " -> "spider man no way home"
       Example: "WALL·E" -> "wall e" */
	btrim(
	  regexp_replace(
	    lower(t.title),          -- lowercase the raw title for case-insensitive comparison
	    '[^a-z0-9]+',            -- replace any run of non-alphanumeric chars with a space
	    ' ',                     -- use a single space as the replacement
	    'g'                      -- apply the replacement globally (all occurrences)
	  )
	) AS film_norm
  FROM musicbrainz.tmdb_movies_2015_2025_staging t
  WHERE t.title IS NOT NULL            -- exclude NULL titles
    AND btrim(t.title) <> ''           -- exclude titles that are only whitespace
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  tmdb_id,
  film_title_raw,
  film_norm,
  /* Count letters+digits only (spaces removed):
     Example: "spider man" -> 9 */
  length(replace(film_norm, ' ', '')) AS film_char_ct,
  /* Count tokens (words) by splitting on whitespace:
     Example: "spider man no way home" -> 5 */
  array_length(regexp_split_to_array(film_norm, '[[:space:]]+'), 1) AS film_token_ct
FROM base
/* Drop normalized titles that collapse to <=1 alphanumeric char:
   Example: "!" -> "" or "x" -> "x" */
WHERE length(replace(film_norm, ' ', '')) > 1;

CREATE INDEX tmdb_film_norm_2015_2025_tmdb_id_idx ON musicbrainz.tmdb_film_norm_2015_2025 (tmdb_id);

CREATE INDEX tmdb_film_norm_2015_2025_char_ct_idx ON musicbrainz.tmdb_film_norm_2015_2025 (film_char_ct);

COMMIT;

select * from musicbrainz.tmdb_film_norm_2015_2025 limit 5;

select count(*) from musicbrainz.tmdb_film_norm_2015_2025;
-- 31,927

/* ============================================================================
STEP 3.2: Build a normalized album-title table for the candidate album universe
-------------------------------------------------------------------------------
GOAL:
  Precompute normalized release_group names once (avoid repeated regexp_replace).
SCOPE:
  Restrict to MusicBrainz release_groups that are explicitly marked as
  Soundtrack via release_group_secondary_type_join (no materialized views).
OUTPUT:
  musicbrainz.soundtrack_rg_norm_2015_2025_us
GRAIN:
  One row per soundtrack release_group_id.
COLUMNS:
  - release_group_id
  - album_title_raw
  - album_norm (lowercase, alnum-only, spaces)
NOTES:
  - Normalization lowercases FIRST, then strips non [a-z0-9] to prevent dropping
    uppercase letters (e.g., "Half-Life" → "half life", not "alf ife").
  - This step does not enforce any date/year/country filters; it defines the
    broadest “soundtrack album universe” available from MusicBrainz metadata.
============================================================================ */

DROP TABLE IF EXISTS musicbrainz.soundtrack_rg_norm_2015_2025_us;

CREATE TABLE musicbrainz.soundtrack_rg_norm_2015_2025_us AS
SELECT DISTINCT
       rg.id AS release_group_id,
       rg.name AS album_title_raw,
       btrim(
         regexp_replace(
           lower(rg.name),       -- lowercase for case-insensitive matching
           '[^a-z0-9]+',         -- replace runs of punctuation/symbols with a space
           ' ',                  -- collapse to a single space
           'g'                   -- global replace (all occurrences)
         )
       ) AS album_norm
FROM musicbrainz.release_group rg
JOIN musicbrainz.release_group_secondary_type_join rgstj
  ON rgstj.release_group = rg.id -- join release group to its secondary-type assignments
JOIN musicbrainz.release_group_secondary_type rgst
  ON rgst.id = rgstj.secondary_type -- resolve secondary-type id to name
WHERE lower(rgst.name) = 'soundtrack'; -- keep only release groups labeled as Soundtrack
CREATE INDEX soundtrack_rg_norm_2015_2025_us_rg_id_idx
  ON musicbrainz.soundtrack_rg_norm_2015_2025_us (release_group_id);

COMMIT;

select * from musicbrainz.soundtrack_rg_norm_2015_2025_us limit 10;

select count(*) from musicbrainz.soundtrack_rg_norm_2015_2025_us
--88,119

COMMIT;

st.markdown("""
### CELL 6 — Title containment candidate generation
""")

st.markdown("""
Generates containment\\-based film–album candidate pairs, excluding films already matched via IMDb\\-exact logic, and applies strict indicator\\-based gating\\.
""")


/* ============================================================================
STEP 3.3: Strict containment candidate pool (indicator-gated)
Goal: Generate a high-precision set of (tmdb_id, release_group_id) candidates using
      normalized title containment + lightweight “soundtrack/score indicator” gating.
Flow: (1) build containment pairs, (2) exclude already IMDb-exact matched films,
      (3) apply gating + insert with match_method = 'title_contains_strict'.
Output: Inserts into musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025.
============================================================================ */

TRUNCATE TABLE musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025;

/* ---------------------------------------------------------------------------
3.3.1: Build raw containment candidates (TEMP table)
Why: Generate film ↔ soundtrack release-group pairs where album_norm contains film_norm.
Notes:
  - This is intentionally “wide” (lots of candidates); later steps will filter/gate.
  - Using normalized text avoids case/punctuation issues during containment matching.
--------------------------------------------------------------------------- */

DROP TABLE IF EXISTS tmp_title_contains_pairs;

CREATE TEMP TABLE tmp_title_contains_pairs AS
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  f.tmdb_id,
  f.film_norm,
  f.film_char_ct,
  a.release_group_id,
  a.album_norm
FROM musicbrainz.tmdb_film_norm_2015_2025 f
JOIN musicbrainz.soundtrack_rg_norm_2015_2025_us a
  ON a.album_norm LIKE '%' || f.film_norm || '%' -- Containment match: album title contains film title.
WHERE f.film_char_ct > 1; -- Guardrail: skip film titles that normalize to ~nothing.
-- 3 mins 46 sec

COMMIT;

select * from tmp_title_contains_pairs;

/* ---------------------------------------------------------------------------
3.3.2: Exclude films already matched via IMDb exact (TEMP table)
Why: Keep candidates “incremental” by dropping any tmdb_id that already has a match row.
Notes:
  - NOT EXISTS is an anti-join: “keep it only if no row exists in the matches table.”
--------------------------------------------------------------------------- */

DROP TABLE IF EXISTS tmp_title_contains_unmatched;

CREATE TEMP TABLE tmp_title_contains_unmatched AS
SELECT p.*
FROM tmp_title_contains_pairs p
WHERE NOT EXISTS (
  SELECT 1
  FROM musicbrainz.tmdb_mb_soundtrack_matches_2015_2025 m
  WHERE m.tmdb_id = p.tmdb_id -- Any match row is enough to exclude the film.
);
-- 4 mins, 3s

select * from tmp_title_contains_unmatched;

/* ---------------------------------------------------------------------------
3.3.3: Apply strict gating + insert candidates into the simplified table
Why: Reduce false positives by requiring either:
  - exact normalized title equality, OR
  - album_norm starts with the film title AND contains a soundtrack indicator token.
Notes:
  - Regex '^' means “start of string”; the boundary group handles space/end/punctuation.
  - ON CONFLICT keeps inserts idempotent per (tmdb_id, release_group_id).
--------------------------------------------------------------------------- */
truncate musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025;

INSERT INTO musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025
  (tmdb_id, release_group_id, match_method, matched_at, soundtrack_type)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  u.tmdb_id,
  u.release_group_id,
  'title_contains_strict' AS match_method, -- Provenance label for this rule.
  now() AS matched_at, -- Load timestamp.
  CASE -- Simple classification from the raw release_group name (QA/EDA bucketing).
    WHEN lower(rg.name) LIKE '%inspired by%' THEN 'inspired_by'
    WHEN lower(rg.name) LIKE '%score%' OR lower(rg.name) LIKE '%orchestral score%' THEN 'score'
    WHEN lower(rg.name) LIKE '%soundtrack%' OR lower(rg.name) LIKE '%music from%' OR lower(rg.name) LIKE '%songs from%' THEN 'songs'
    WHEN (lower(rg.name) LIKE '%from the motion picture%' OR lower(rg.name) LIKE '%from the original motion picture%')
         AND lower(rg.name) NOT LIKE '%soundtrack%'
         AND lower(rg.name) NOT LIKE '%score%' THEN 'single'
    ELSE 'unknown'
  END AS soundtrack_type
FROM tmp_title_contains_unmatched u
JOIN musicbrainz.release_group rg
  ON rg.id = u.release_group_id -- Pull raw title for soundtrack_type heuristics.
# auto-detected possible Altair chart: WHERE
try:
    st.altair_chart(WHERE, use_container_width=True)
except Exception:
    st.write(WHERE)
  u.album_norm ~ '^[a-z0-9[:space:][:punct:]]+$' -- Sanity check: expected normalized charset.
  AND (
    u.album_norm = u.film_norm -- Exact normalized title match.
    OR (
      u.album_norm ~ ('^' || u.film_norm || '([[:space:]]|$|[[:punct:]])') -- Film title at start (with boundary).
      AND u.album_norm ~ '(soundtrack|original soundtrack|original score|score|original motion picture|motion picture|music from|ost|music composed by|music by)' -- Soundtrack indicator gating.
    )
  )
ON CONFLICT (tmdb_id, release_group_id, match_method) DO NOTHING; -- Idempotent insert per composite key.

commit;

select * from musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 limit 10;

select count(*) from musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025;
-- 16410

st.markdown("""
### CELL 7 — Candidate pruning passes
""")

st.markdown("""
Applies precision\\-focused pruning to remove false positives \\(collision\\-prone titles, episodic/variant releases, franchise sequels, etc\\.\\)\\.
""")

/* ============================================================================
STEP 3.4: Enforce TEMPLATE-ONLY rules for collision-prone single-word film titles
-------------------------------------------------------------------------------
GOAL:
  Aggressively prune strict-containment candidates for collision-prone single-word
  film titles (e.g., Run, War, Air, Room, Mother, Lion, After) by only retaining
  album titles that look like canonical soundtrack/score naming templates.

WHY THIS STEP EXISTS:
  For single-word titles, substring containment generates many false positives
  because the film token often appears in unrelated album names (e.g., "Room of
  Swords", "Mother Earthbound", "After Hours"). True film soundtrack/score albums
  more often use standardized naming templates such as:
    - "Original Motion Picture Soundtrack"
    - "Original Score"
    - "Music From the Motion Picture/Film"
  This step trades recall for precision by allowing only:
    (a) exact title-only albums, or
    (b) title-prefixed albums that match a strict soundtrack-template pattern.

APPLICABILITY:
  This rule applies when:
    - film_token_ct = 1 (single-word titles), AND
    - film_char_ct <= 6  (tunable collision threshold)
  (Adjust the threshold if pruning is too aggressive.)

KEEP CRITERIA (candidate survives if ANY are true):
  1) Direct title-only match:
       album_norm = film_norm
     Example: film "run" → album "run"

  2) Title + optional delimiter + strict soundtrack template:
     album_norm must:
       - start with the film title token, and
       - optionally include a delimiter right after the title (":", "-", "(", "["),
       - optionally include the word "official",
       - then contain ONLY a controlled set of soundtrack/score phrases, and
       - then end (or close a bracket/parenthesis) with no additional free text.
     Examples kept:
       - "run: original motion picture soundtrack"
       - "room (original score)"
       - "after - music from the film"
     Examples deleted:
       - "room of swords season 1 ost"  (extra words, not a film template)
       - "mother earthbound zero soundtrack" (extra tokens)
       - "after hours ... ost" (not a canonical film template)

DELETE RULE:
  Any candidate in tmdb_mb_soundtrack_candidates_2015_2025 that fails the KEEP
  criteria above is removed.

IMPLEMENTATION:
  DELETE-based pruning over the output of STEP 3.3 for match_method='title_contains_strict'.

NOTES:
  - This step intentionally trades recall for precision for collision-prone single-word titles.
  - Multi-word film titles are not affected by this step.
============================================================================ */

DELETE FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
USING musicbrainz.tmdb_film_norm_2015_2025 f,
      musicbrainz.soundtrack_rg_norm_2015_2025_us a
WHERE c.tmdb_id = f.tmdb_id
  AND c.release_group_id = a.release_group_id
  AND c.match_method = 'title_contains_strict'
  AND f.film_token_ct = 1
  -- AND f.film_char_ct <= 6
  AND NOT (
    /* Allow exact title-only album. */
    a.album_norm = f.film_norm
    /* Otherwise require strict "title + delimiter + soundtrack template". */
    OR a.album_norm ~ (
      '^' || f.film_norm ||
      '[[:space:]]*[:\\-\\(\\[]?[[:space:]]*' ||
      '(official[[:space:]]+)?' ||
      '(' ||
        'original[[:space:]]+motion[[:space:]]+picture[[:space:]]+soundtrack' ||
        '|original[[:space:]]+soundtrack' ||
        '|original[[:space:]]+motion[[:space:]]+picture[[:space:]]+score' ||
        '|original[[:space:]]+score' ||
        '|music[[:space:]]+from[[:space:]]+the[[:space:]]+motion[[:space:]]+picture' ||
        '|music[[:space:]]+from[[:space:]]+and[[:space:]]+inspired[[:space:]]+by[[:space:]]+the[[:space:]]+motion[[:space:]]+picture' ||
        '|music[[:space:]]+from[[:space:]]+the[[:space:]]+film' ||
        '|music[[:space:]]+from[[:space:]]+and[[:space:]]+inspired[[:space:]]+by[[:space:]]+the[[:space:]]+film'
      || ')' ||
      '[[:space:]]*([\\)\\]]|$)'
    )
  );

COMMIT;

select count(*) from musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025;
-- 8511

/* ============================================================================
STEP 3.5: Prune non-film soundtrack variants from strict containment candidates
-------------------------------------------------------------------------------
GOAL:
  Remove high-confidence false positives from the strict-containment candidate
  pool that are clearly *not* feature-film soundtracks for the TMDB film.

WHY THIS STEP EXISTS:
  Even after enforcing title containment and canonical soundtrack templates,
  shared titles and franchises still collide with:
    1) episodic / TV / streaming series soundtracks (e.g., "Season 1", "Netflix")
    2) video game, anime, OVA, platform-specific, and non-film releases
    3) shared-universe / crossover / animated-universe releases that reuse the
       same franchise title but clearly refer to a different work (e.g.,
       "DC Universe Animated Original Movie", "vs", "crossover").

INPUT:
  musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025
  musicbrainz.soundtrack_rg_norm_2015_2025_us

APPLICABILITY:
  - match_method = 'title_contains_strict' only
  - imdb_exact matches are never affected

DELETE RULE:
  Delete candidates where album_norm contains strong non-film signals, including:
    • episodic / series indicators:
        season, episode, series, tv, television,
        netflix series, prime video, apple tv, podcast, radio
    • non-film media / variant indicators:
        game, video game, ova, demo, prototype,
        remaster, remastered, beta, 8bit, platforms (ps5/xbox/steam/pc), etc.
    • shared-universe / crossover indicators:
        dc universe, animated original movie, crossover, vs/versus, etc.

OUTPUT:
  A cleaner, film-focused strict-containment candidate pool suitable for
  downstream ranking or promotion logic.

NOTES:
  - Keyword lists are heuristic and may be refined iteratively as edge cases
    are reviewed.
  - This step favors precision over recall; if you later want to support
    TV specials, animated-universe films, or games, gate/branch these prunes.
============================================================================ */


/* --- 3.5A: Episodic / TV / series indicators --- */
DELETE FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
USING musicbrainz.soundtrack_rg_norm_2015_2025_us a
WHERE c.release_group_id = a.release_group_id
  AND c.match_method = 'title_contains_strict'
  AND a.album_norm ~ '(^|[[:space:]])(season|seasons|episode|episodes|series|tv|television|prime video|netflix series|apple tv|anime|manga|visual novel|vn|radio|podcast|special|anniversary|documentary)([[:space:]]|$)';


/* --- 3.5B: Games / platforms / non-film variants --- */
DELETE FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
USING musicbrainz.soundtrack_rg_norm_2015_2025_us a
WHERE c.release_group_id = a.release_group_id
  AND c.match_method = 'title_contains_strict'
  AND a.album_norm ~ '(^|[[:space:]])(game|video game|8 bit|8bit|remaster|remastered|prototype|beta|demo|ova|game gear|knuckles|cd[[:space:]]~|karaoke|cover|tribute|remix|mix|compilation|live|concert|musical|broadway|switch|ps4|ps5|xbox|steam|pc|trailer|teaser|promo|promotional|tv spot|advert|advertisement|club|festival|venue|sessions|alternate|alternative|concept album|unofficial|fan made)([[:space:]]|$)';


/* --- 3.5C: Shared-universe / crossover / animated-universe collisions --- */
DELETE FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
USING musicbrainz.soundtrack_rg_norm_2015_2025_us a
WHERE c.release_group_id = a.release_group_id
  AND c.match_method = 'title_contains_strict'
  AND (
    /* DC/Marvel universe-packaging patterns */
    a.album_norm ~ '(^|[[:space:]])(dc[[:space:]]+universe|marvel)([[:space:]]|$)'
    OR a.album_norm ~ '(^|[[:space:]])(animated[[:space:]]+original[[:space:]]+movie|original[[:space:]]+movie)([[:space:]]|$)'
    /* Crossovers */
    OR a.album_norm ~ '(^|[[:space:]])(vs|versus|crossover|universe|collection)([[:space:]]|$)'
    OR a.album_norm ~ '(^|[[:space:]])x([[:space:]]|$)'
  );

COMMIT;

select count(*) from musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025;
-- 8105

/* ============================================================================
STEP 3.6: Franchise sequel pruning (numbered / Part / Chapter / Volume)
-------------------------------------------------------------------------------
GOAL:
  Remove title-contains candidates where the album title clearly refers to a
  different entry in a franchise (e.g., film "Sonic the Hedgehog" incorrectly
  matching album "Sonic the Hedgehog 2", "Sonic the Hedgehog 3", etc.).

WHY THIS STEP EXISTS:
  Strict containment alone cannot distinguish "base title" vs. "sequels" when
  the base title is a prefix of sequel titles. This produces high-confidence
  false positives for well-known franchises.

WHAT THIS PRUNES (EXAMPLES):
  - "sonic the hedgehog"  → "sonic the hedgehog 2 ..."
  - "the accountant"      → "the accountant 2 ..."
  - "greenland"           → "greenland 2 ..."
  - "halloween"           → "halloween iii ..."
  - "mission impossible"  → "mission impossible 2 ..." (if present)

RULE (SAFE, EXPLAINABLE):
  If the TMDB film title does NOT itself look like a numbered/part/chapter/vol
  entry, then delete candidates where the album title begins with the film title
  and immediately continues with a sequel marker such as:
    - digits 2–9 (as a standalone token)
    - roman numerals ii–x (as a standalone token)
    - "part", "chapter", "volume", "vol" (+ optional number/roman numeral)

IMPORTANT NOTE:
  This step intentionally focuses on explicit, structured sequel markers.
  It does NOT attempt to catch subtitle sequels like "Halloween Kills" or
  "After We Fell" (those can be handled in a separate, more opinionated step).
============================================================================ */

DELETE FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
USING musicbrainz.tmdb_film_norm_2015_2025 f,
      musicbrainz.soundtrack_rg_norm_2015_2025_us a
WHERE c.tmdb_id = f.tmdb_id
  AND c.release_group_id = a.release_group_id
  AND c.match_method = 'title_contains_strict'
  /* Only prune when the FILM is NOT already a numbered/part/chapter/vol entry. */
  AND f.film_norm !~ '(^|[[:space:]])(part|chapter|volume|vol|[2-9]|ii|iii|iv|v|vi|vii|viii|ix|x)([[:space:]]|$)'
  /* Album must start with the film title and then immediately indicate sequel-ness. */
  AND a.album_norm ~ (
        '^' || f.film_norm ||
        '[[:space:]]*([:\\-\\(\\[])?[[:space:]]*' ||
        '(' ||
          '([2-9])' ||
          '|(ii|iii|iv|v|vi|vii|viii|ix|x)' ||
          '|(part[[:space:]]+([2-9]|ii|iii|iv|v|vi|vii|viii|ix|x|one|two|three|four|five|six|seven|eight|nine))' ||
          '|(chapter[[:space:]]+([2-9]|ii|iii|iv|v|vi|vii|viii|ix|x|one|two|three|four|five|six|seven|eight|nine))' ||
          '|(volume[[:space:]]+([2-9]|ii|iii|iv|v|vi|vii|viii|ix|x|one|two|three|four|five|six|seven|eight|nine))' ||
          '|(vol[[:space:]]*\\.?[[:space:]]*([2-9]|ii|iii|iv|v|vi|vii|viii|ix|x))' ||
        ')' ||
        '([[:space:]]|$)'
      );

COMMIT;

select count(*) from musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025;
-- 8077

st.markdown("""
### CELL 8 — Year\\-based pruning \\+ overall coverage
""")

st.markdown("""
Introduces effective earliest\\-release\\-year logic \\(US\\-first, global fallback\\), removes implausible temporal matches, and recalculates overall soundtrack coverage\\.
""")


/* ============================================================================
STEP 3.7: Compute an “effective earliest release year” for candidate release groups + prune year-mismatches
-------------------------------------------------------------------------------
GOAL:
  For each candidate soundtrack release_group_id, compute a stable “first availability”
  year from MusicBrainz release_event data, then prune candidates whose TMDB film year
  is clearly incompatible.

WHAT CHANGED (vs prior min/max bounds approach):
  - We no longer compute (mb_min_year, mb_max_year) because MAX(year) is reissue-sensitive.
  - Instead we compute:
      • mb_min_year_us: earliest US release_event year (re.country = 222), if present
      • mb_min_year_global: earliest release_event year anywhere, as a fallback
      • mb_effective_min_year: COALESCE(us, global) = best proxy for earliest availability

WHY THIS STEP EXISTS:
  Title-based matching can produce cross-era collisions (shared titles, remasters, compilations).
  Using an earliest-year proxy helps remove obvious mismatches while staying NULL-safe when
  MusicBrainz has incomplete date metadata.

SCHEMA NOTE:
  Release years come from `musicbrainz.release_event.date_year` joined through `musicbrainz.release`.

SCOPE:
  - Only release_group_ids present in tmdb_mb_soundtrack_candidates_2015_2025 (keeps this step fast).
  - No reliance on any materialized views.

OUTPUT TABLE:
  musicbrainz.soundtrack_rg_year_bounds_candidates
    - release_group_id
    - mb_min_year_us
    - mb_min_year_global
    - mb_effective_min_year
    - us_release_event_year_ct
    - global_release_event_year_ct
    - release_ct

PRUNING RULE (NULL-SAFE, reissue-resistant):
  Delete candidate iff:
    - TMDB film year is known (t.year IS NOT NULL), AND
    - effective min year is known (mb_effective_min_year IS NOT NULL), AND
    - t.year is outside ±1 year of mb_effective_min_year.

INTERPRETATION:
  If mb_effective_min_year is NULL, MB lacks usable year data for that release_group, so we
  KEEP the candidate rather than pruning on missing metadata alone.
============================================================================ */

DROP TABLE IF EXISTS musicbrainz.soundtrack_rg_year_bounds_candidates;

CREATE TABLE musicbrainz.soundtrack_rg_year_bounds_candidates AS
WITH candidate_rgs AS (
  SELECT DISTINCT c.release_group_id
  FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
  WHERE c.match_method = 'title_contains_strict'
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  r.release_group AS release_group_id,
  MIN(re.date_year)::int AS mb_min_year,
  MAX(re.date_year)::int AS mb_max_year,
  COUNT(re.date_year) AS release_event_year_ct,
  COUNT(DISTINCT r.id) AS release_ct
FROM candidate_rgs crg
JOIN musicbrainz.release r
  ON r.release_group = crg.release_group_id
LEFT JOIN musicbrainz.release_event re
  ON re.release = r.id
 AND re.date_year IS NOT NULL
GROUP BY r.release_group;

CREATE INDEX soundtrack_rg_year_bounds_candidates_rg_id_idx
  ON musicbrainz.soundtrack_rg_year_bounds_candidates (release_group_id);

COMMIT;

/* ============================================================================
STEP 3.7B: Prune candidates with incompatible release years (NULL-safe)
-------------------------------------------------------------------------------
GOAL:
  Remove title-contains candidates where the TMDB film release year is clearly
  incompatible with the MusicBrainz release_group's known release years.

RULE (NULL-SAFE):
  Delete candidate iff:
    - TMDB year is known (t.year IS NOT NULL), AND
    - MB year bounds are known (mb_min_year IS NOT NULL AND mb_max_year IS NOT NULL), AND
    - TMDB year falls outside the allowed window around MB bounds.

DEFAULT WINDOW:
  ±1 year tolerance against the MB year range:
    Keep if: t.year BETWEEN (mb_min_year - 1) AND (mb_max_year + 1)
    Delete otherwise.

WHY THIS IS SAFE:
  If MB has missing year metadata (NULL bounds), we KEEP the candidate rather
  than risk pruning a legitimate match due to incomplete MusicBrainz dates.

INPUT:
  - musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025
  - musicbrainz.tmdb_movies_2015_2025_staging
  - musicbrainz.soundtrack_rg_year_bounds_candidates

OUTPUT:
  A reduced candidate pool with fewer obvious cross-era collisions.

NOTES:
  - This step is usually higher impact for very common titles/franchises.
  - If you later decide recall is too low, widen the window (±2) or gate it by
    popularity ("Vote Count") so only high-signal films get pruned.
============================================================================ */

DELETE FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
USING musicbrainz.tmdb_movies_2015_2025_staging t,
      musicbrainz.soundtrack_rg_year_bounds_candidates y
WHERE c.tmdb_id = t.id
  AND c.release_group_id = y.release_group_id
  AND c.match_method = 'title_contains_strict'
  AND t.year IS NOT NULL
  AND y.mb_min_year IS NOT NULL
  AND y.mb_max_year IS NOT NULL
  AND NOT (
    t.year BETWEEN (y.mb_min_year - 1) AND (y.mb_max_year + 1)
  );

--delete ABOVE

/* ============================================================================
STEP 3.7A: Build effective earliest release year for candidate release groups (US-first, global fallback)
-------------------------------------------------------------------------------
GOAL:
  For each candidate soundtrack release_group_id, compute:
    - mb_min_year_us: earliest US release_event year (if recorded)
    - mb_min_year_global: earliest release_event year anywhere
    - mb_effective_min_year: COALESCE(us, global) as the best “first availability” proxy
WHY:
  Using MAX(year) is reissue-sensitive and creates decades-late artifacts; earliest-year is more stable.
OUTPUT:
  musicbrainz.soundtrack_rg_year_bounds_candidates (effective earliest-year fields)
============================================================================ */
DROP TABLE IF EXISTS musicbrainz.soundtrack_rg_year_bounds_candidates;

CREATE TABLE musicbrainz.soundtrack_rg_year_bounds_candidates AS
WITH candidate_rgs AS (
  SELECT DISTINCT c.release_group_id -- DISTINCT because the candidates table is many rows per release_group_id (many films can point to the same album).
  FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
),
global_events AS (
  SELECT re.release, re.date_year -- Keep only the two fields we need: which release the event belongs to, and the year.
  FROM musicbrainz.release_event re
  WHERE re.date_year IS NOT NULL -- Ignore undated events so MIN()/COUNT() reflect only usable year data.
),
us_events AS (
  SELECT re.release, re.date_year
  FROM musicbrainz.release_event re
  WHERE re.date_year IS NOT NULL
    AND re.country = 222 -- Restrict to US release events (in this dump, 222 = United States in musicbrainz.area).
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  r.release_group AS release_group_id, -- We report one row per release_group (album), not per individual release.
  MIN(ue.date_year)::int AS mb_min_year_us, -- Earliest US year across all releases in the group (NULL if no US-dated events exist).
  MIN(ge.date_year)::int AS mb_min_year_global, -- Earliest year anywhere across all releases in the group (acts as a fallback).
  COALESCE(MIN(ue.date_year), MIN(ge.date_year))::int AS mb_effective_min_year, -- “Best” earliest year: use US if present, else global.
  COUNT(ue.date_year) AS us_release_event_year_ct, -- How many US-dated release_event rows contributed (coverage / data-quality signal).
  COUNT(ge.date_year) AS global_release_event_year_ct, -- How many dated release_event rows exist globally (coverage / data-quality signal).
  COUNT(DISTINCT r.id) AS release_ct -- How many distinct releases exist under the release_group (avoids double-counting from multiple events).
FROM candidate_rgs crg
JOIN musicbrainz.release r
  ON r.release_group = crg.release_group_id -- Expand each release_group to its releases so we can look at release_event years.
LEFT JOIN us_events ue
  ON ue.release = r.id -- LEFT JOIN: keep the release even if it has no US event rows (mb_min_year_us can be NULL).
LEFT JOIN global_events ge
  ON ge.release = r.id -- LEFT JOIN: keep the release even if it has no dated events at all (global year can be NULL).
GROUP BY r.release_group;


CREATE INDEX soundtrack_rg_year_bounds_candidates_rg_id_idx
  ON musicbrainz.soundtrack_rg_year_bounds_candidates (release_group_id);

COMMIT;

select * from musicbrainz.soundtrack_rg_year_bounds_candidates;
/* ============================================================================
STEP 3.7B: Prune candidates with incompatible release years (NULL-safe, reissue-resistant)
-------------------------------------------------------------------------------
GOAL:
  Remove strict-containment candidates where the film year is clearly incompatible with the
  soundtrack release group’s effective earliest year (US-first, global fallback).
RULE (NULL-SAFE):
  Delete candidate iff:
    - film year is known, AND effective min year is known, AND
    - film year is outside ±1 year of the effective earliest year.
WHY:
  This avoids the “MAX-year reissue” problem where late reissues keep bad candidates alive.
============================================================================ */
DELETE FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
USING musicbrainz.tmdb_movies_2015_2025_staging t,
      musicbrainz.soundtrack_rg_year_bounds_candidates y
WHERE c.tmdb_id = t.id
  AND c.release_group_id = y.release_group_id
  AND c.match_method = 'title_contains_strict'
  AND t.year IS NOT NULL
  AND y.mb_effective_min_year IS NOT NULL
  AND NOT (
    t.year BETWEEN (y.mb_effective_min_year - 1) AND (y.mb_effective_min_year + 1)
  );


COMMIT;

select count(*) from musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025;
-- 3802  --> goes down to 3683


-- Let's look at the data
SELECT t.id AS tmdb_id,
       t.title AS film_title,
       t."Release Date" AS film_release_date,
       t.genres AS film_genres,
       t."Vote Count" AS film_vote_count,
       c.release_group_id,
       rg.name AS album_title,
       c.match_method,
       c.match_score
FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c
JOIN musicbrainz.tmdb_movies_2015_2025_staging t ON t.id = c.tmdb_id
JOIN musicbrainz.release_group rg ON rg.id = c.release_group_id
-- where length(t.title) < 4
ORDER BY t."Vote Count" DESC NULLS LAST
LIMIT 5000;



/* ============================================================================
Step 3.8 Validate overall match coverage (final matches + retained candidates)
Goal: Measure what % of TMDB films (2015–2025 pull) have at least one row in either:
      - tmdb_mb_soundtrack_matches_2015_2025 (IMDb-exact matches), OR
      - tmdb_mb_soundtrack_candidates_2015_2025 (heuristic candidates retained after pruning),
      reported by TMDB vote-count bucket.
Matched definition: a film is “matched” if it has ≥1 row in matches OR candidates.
Notes: This is coverage only (not confidence); candidates are not guaranteed correct.
============================================================================ */


/* ---------------------------------------------------------------------------
3.8.1: Bucket TMDB films by vote_count (TEMP table)
Why: Reuse the same bucket framework as Step 2.3 so coverage comparisons are apples-to-apples.
Notes:
  - TEMP tables live only for this session/connection.
  - COALESCE("Vote Count", 0) treats NULL as 0 so every film lands in a bucket.
--------------------------------------------------------------------------- */

DROP TABLE IF EXISTS tmdb_movies_bucketed;

CREATE TEMP TABLE tmdb_movies_bucketed AS
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  id AS tmdb_id,
  COALESCE("Vote Count", 0) AS vote_count, -- Normalize NULL vote counts to 0 so every film can be bucketed.
  CASE -- vote_count_range: bucket label used for grouping/display.
    WHEN COALESCE("Vote Count", 0) BETWEEN 0 AND 100 THEN '0-100'
    WHEN COALESCE("Vote Count", 0) BETWEEN 101 AND 250 THEN '101-250'
    WHEN COALESCE("Vote Count", 0) BETWEEN 251 AND 500 THEN '251-500'
    WHEN COALESCE("Vote Count", 0) BETWEEN 501 AND 750 THEN '501-750'
    WHEN COALESCE("Vote Count", 0) BETWEEN 751 AND 1000 THEN '751-1000'
    WHEN COALESCE("Vote Count", 0) BETWEEN 1001 AND 1500 THEN '1001-1500'
    WHEN COALESCE("Vote Count", 0) BETWEEN 1501 AND 2000 THEN '1501-2000'
    WHEN COALESCE("Vote Count", 0) BETWEEN 2001 AND 3000 THEN '2001-3000'
    WHEN COALESCE("Vote Count", 0) BETWEEN 3001 AND 5000 THEN '3001-5000'
    WHEN COALESCE("Vote Count", 0) BETWEEN 5001 AND 7500 THEN '5001-7500'
    WHEN COALESCE("Vote Count", 0) BETWEEN 7501 AND 10000 THEN '7501-10000'
    ELSE '10001+'
  END AS vote_count_range,
  CASE -- vote_count_sort: numeric sort key so buckets order logically (not alphabetically).
    WHEN COALESCE("Vote Count", 0) BETWEEN 0 AND 100 THEN 1
    WHEN COALESCE("Vote Count", 0) BETWEEN 101 AND 250 THEN 2
    WHEN COALESCE("Vote Count", 0) BETWEEN 251 AND 500 THEN 3
    WHEN COALESCE("Vote Count", 0) BETWEEN 501 AND 750 THEN 4
    WHEN COALESCE("Vote Count", 0) BETWEEN 751 AND 1000 THEN 5
    WHEN COALESCE("Vote Count", 0) BETWEEN 1001 AND 1500 THEN 6
    WHEN COALESCE("Vote Count", 0) BETWEEN 1501 AND 2000 THEN 7
    WHEN COALESCE("Vote Count", 0) BETWEEN 2001 AND 3000 THEN 8
    WHEN COALESCE("Vote Count", 0) BETWEEN 3001 AND 5000 THEN 9
    WHEN COALESCE("Vote Count", 0) BETWEEN 5001 AND 7500 THEN 10
    WHEN COALESCE("Vote Count", 0) BETWEEN 7501 AND 10000 THEN 11
    ELSE 12
  END AS vote_count_sort
FROM musicbrainz.tmdb_movies_2015_2025_staging;

SELECT * FROM tmdb_movies_bucketed ORDER BY vote_count_sort;


/* ---------------------------------------------------------------------------
3.7B COMMAND 2: Aggregate overall coverage by vote_count bucket (matches OR candidates)
Why: Quantify incremental coverage after heuristics by treating a film as “covered”
     if it appears in either persisted table.
Notes:
  - EXISTS prevents double-counting when one film maps to many albums (many rows per tmdb_id).
  - We start from the full TMDB bucketed set so films with zero coverage still count in the denominator.
--------------------------------------------------------------------------- */

# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  b.vote_count_range,
  COUNT(*) AS films, -- Total films in this vote_count bucket.
  SUM(
    CASE WHEN
      EXISTS (SELECT 1 FROM musicbrainz.tmdb_mb_soundtrack_matches_2015_2025 m WHERE m.tmdb_id = b.tmdb_id)
      OR EXISTS (SELECT 1 FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c WHERE c.tmdb_id = b.tmdb_id)
    THEN 1 ELSE 0 END
  ) AS matched_films, -- Films with ≥1 row in matches OR candidates.
  ROUND(
    100.0 * SUM(
      CASE WHEN
        EXISTS (SELECT 1 FROM musicbrainz.tmdb_mb_soundtrack_matches_2015_2025 m WHERE m.tmdb_id = b.tmdb_id)
        OR EXISTS (SELECT 1 FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025 c WHERE c.tmdb_id = b.tmdb_id)
      THEN 1 ELSE 0 END
    )::numeric / NULLIF(COUNT(*), 0),
    2
  ) AS match_rate_pct -- Percent of films in the bucket that have ≥1 row in matches OR candidates.
FROM tmdb_movies_bucketed b
GROUP BY b.vote_count_range, b.vote_count_sort
ORDER BY b.vote_count_sort;

/*
 *  Range		Film Ct		Match	%
 * 	10-100		24088		1986	8.24
 * 	101-250		3589		663		18.47
 * 	251-500		1699		478		28.13
 * 	501-750		671			292		43.52
 * 	751-1000	367			160		43.60
 * 	1001-1500	443			273		61.63
 * 	1501-2000	248			164		66.13
 * 	2001-3000	284			222		78.17
 * 	3001-5000	252			219		86.90
 * 	5001-7500	140			132		94.29
 * 	7501-10000	77			74		96.10
 * 	10001+		113			113		100.00
 *  Total       31971	4776		14.94
 */

st.markdown("""
### CELL 9 — Materialized view definition \\(film→soundtrack spine\\)
""")

st.markdown("""
Defines the canonical materialized view that unifies final matches and retained candidates, enriched with TMDB and MusicBrainz metadata\\. This view serves as the export surface for downstream Python notebooks \\(3\\.1, 3\\.2\\)\\.
""")

/*****************************
 * PART 4. MATERIALIZED VIEW
 *****************************/

/* =============================================================================
STEP 4.1 MATERIALIZED VIEW: musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025
--------------------------------------------------------------------------------
PURPOSE:
  Build the project’s “film → soundtrack” spine for TMDB films (2015–2025),
  combining BOTH:
    (a) high-confidence identifier matches (IMDb/Wikidata, etc.), and
    (b) retained heuristic candidates (title containment after pruning).

  This MV is the primary export surface for Python EDA:
  one row per film ↔ release_group relationship, plus convenient enrichment
  columns from both TMDB and MusicBrainz.

NON-NEGOTIABLE RULE:
  - This MV must NOT filter out any row already present in either:
      • tmdb_mb_soundtrack_matches_2015_2025, or
      • tmdb_mb_soundtrack_candidates_2015_2025
    Enrichment joins must be LEFT JOINs so missing MusicBrainz metadata never
    deletes a match/candidate row.

DATA SOURCES:
  - musicbrainz.tmdb_movies_2015_2025_staging
      TMDB film metadata (title, dates, popularity proxies, etc.).
  - musicbrainz.tmdb_mb_soundtrack_matches_2015_2025
      “Final” matches (one row per tmdb_id × release_group_id).
  - musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025
      Heuristic candidates (one row per tmdb_id × release_group_id × match_method).

GRAIN:
  One row per (tmdb_id, release_group_id, match_method).
  - Final matches will typically have one match_method (e.g., imdb_exact).
  - Candidates preserve match_method so multiple heuristics can propose the same link.

KEY DESIGN CHOICES (ENRICHMENT):
  1) Canonical release_id (deterministic):
     - We pick min(release.id) per release_group as a stable “canonical” release
       to attach release-level fields (labels, packaging, language, etc.).
     - This is an enrichment convenience; it is not claiming “the” correct release.

  2) US release date enrichment (best-effort, non-filtering):
     - album_us_release_date is the earliest US release_event date observed across
       releases in the release_group (year required; month/day may be missing).
     - We also expose “missing month/day” flags so analysts can decide how strict
       to be in notebooks.

OUTPUT:
  musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025
============================================================================= */

DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025;

CREATE MATERIALIZED VIEW musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025 as
WITH tmdb_films AS (
  -- TMDB film attributes (staging is already scoped to 2015–2025 by your upstream pull).
  -- We alias fields to stable, analysis-friendly names (film_*) and normalize blank IDs to NULL.
  SELECT
    id AS tmdb_id,
    title AS film_title,
    adult AS film_adult,
    "Runtime (min)" AS film_runtime_min,
    genres AS film_genres,
    "Rating (0-10)" AS film_rating,
    COALESCE("Vote Count", 0) AS film_vote_count, -- Treat NULL vote counts as 0 so bucketing/EDA is simpler.
    "MPAA Rating" AS film_mpaa_rating,
    "Original Title" AS film_original_title,
    "Language Name" AS film_language_name,
    NULLIF(btrim("IMDb ID"), '') AS film_imdb_id, -- Normalize empty/whitespace-only IDs to NULL.
    NULLIF(btrim("Wikidata ID"), '') AS film_wikidata_id,
    countries AS film_countries,
    "year" AS film_year,
    "Release Date" AS film_release_date,
    popularity AS film_popularity,
    budget AS film_budget,
    revenue AS film_revenue,
    studios AS film_studios,
    director AS film_director,
    "Soundtrack/Composer" AS film_soundtrack_composer_raw, -- Raw TMDB field; may be sparse/inconsistent.
    "Top Cast" AS film_top_cast,
    keywords AS film_keywords,
    ingested_at AS film_ingested_at
  FROM musicbrainz.tmdb_movies_2015_2025_staging
),
matches_all AS (
  -- Union your two mapping sources into one “edge list”.
  -- Grain: one row per (tmdb_id, release_group_id, match_method) across matches + candidates.
  SELECT
    tmdb_id,
    release_group_id,
    match_method,
    matched_at,
    soundtrack_type,
    NULL::text AS notes -- Matches table has no notes; keep column alignment for UNION ALL.
  FROM musicbrainz.tmdb_mb_soundtrack_matches_2015_2025
  UNION ALL
  SELECT
    tmdb_id,
    release_group_id,
    match_method,
    matched_at,
    soundtrack_type,
    notes
  FROM musicbrainz.tmdb_mb_soundtrack_candidates_2015_2025
),
rg_set AS (
  -- Small “working set” of release_group_ids referenced by any match/candidate row.
  -- This keeps downstream enrichment joins fast (we don’t scan all of MusicBrainz).
  SELECT DISTINCT release_group_id
  FROM matches_all
),
canonical_release_any AS (
  -- Choose one deterministic “canonical release” per release_group (min(id) is stable and reproducible).
  -- We use this release_id to pull release-level metadata (labels, language, packaging, etc.).
  SELECT
    r.release_group AS release_group_id,
    min(r.id) AS release_id
  FROM musicbrainz.release r
  JOIN rg_set s ON s.release_group_id = r.release_group
  GROUP BY r.release_group
),
rg_us_any_missing AS (
  -- Data-quality flags across *all* US release_event rows for the release_group.
  -- Example: if any US event is missing month/day, we expose that so analysts can filter if needed.
  SELECT
    s.release_group_id,
    bool_or(re.date_month IS NULL) AS us_any_event_missing_month,
    bool_or(re.date_day IS NULL) AS us_any_event_missing_day
  FROM rg_set s
  JOIN musicbrainz.release r ON r.release_group = s.release_group_id
  JOIN musicbrainz.release_event re ON re.release = r.id
  WHERE re.country = 222 -- 222 = United States in this MB dump (musicbrainz.area.id)
    AND re.date_year IS NOT NULL
  GROUP BY s.release_group_id
),
rg_first_us_date AS (
  -- Pick the earliest US release_event per release_group (one row per release_group).
  -- DISTINCT ON + ORDER BY is a common Postgres pattern for “first row per group”.
  SELECT DISTINCT ON (s.release_group_id)
    s.release_group_id,
    re.date_year AS album_us_release_year,
    re.date_month AS album_us_release_month_min_observed,
    re.date_day AS album_us_release_day_min_observed,
    make_date(
      re.date_year::int,
      COALESCE(re.date_month::int, 1), -- If month/day missing, default to 1 so we can construct a DATE.
      COALESCE(re.date_day::int, 1)
    ) AS album_us_release_date,
    (re.date_month IS NULL) AS us_date_has_missing_month,
    (re.date_day IS NULL) AS us_date_has_missing_day
  FROM rg_set s
  JOIN musicbrainz.release r ON r.release_group = s.release_group_id
  JOIN musicbrainz.release_event re ON re.release = r.id
  WHERE re.country = 222
    AND re.date_year IS NOT NULL
  ORDER BY
    s.release_group_id,
    make_date(re.date_year::int, COALESCE(re.date_month::int, 1), COALESCE(re.date_day::int, 1)) ASC,
    (re.date_month IS NULL) ASC,
    (re.date_day IS NULL) ASC
),
release_group_secondary_types AS (
  -- Secondary types are “multi-valued”; we roll them up into a single readable string per release_group.
  -- This is convenient for EDA/filtering without extra joins in Python.
  SELECT
    s.release_group_id,
    string_agg(rgst.name, ' | '::text ORDER BY rgst.name) AS rg_secondary_types
  FROM rg_set s
  LEFT JOIN musicbrainz.release_group_secondary_type_join j ON j.release_group = s.release_group_id
  LEFT JOIN musicbrainz.release_group_secondary_type rgst ON rgst.id = j.secondary_type
  GROUP BY s.release_group_id
),
release_group_tags AS (
  -- Release-group tags are also multi-valued. We expose both:
  --  - a pipe-delimited text field for quick scanning, and
  SELECT
    s.release_group_id,
    string_agg(t.name::text, ' | '::text ORDER BY rgt.count DESC, (t.name::text)) AS rg_tags_text
  FROM rg_set s
  LEFT JOIN musicbrainz.release_group_tag rgt ON rgt.release_group = s.release_group_id
  LEFT JOIN musicbrainz.tag t ON t.id = rgt.tag
  GROUP BY s.release_group_id
),
release_tags AS (
  -- Tags at the *release* level (using the canonical release_id chosen above).
  -- These can differ from release_group tags, so we keep both.
  SELECT
    s.release_group_id,
    string_agg(t.name::text, ' | '::text ORDER BY rt.count DESC, (t.name::text)) AS release_tags_text
  FROM rg_set s
  LEFT JOIN canonical_release_any cr ON cr.release_group_id = s.release_group_id
  LEFT JOIN musicbrainz.release_tag rt ON rt.release = cr.release_id
  LEFT JOIN musicbrainz.tag t ON t.id = rt.tag
  GROUP BY s.release_group_id
),
release_labels AS (
  -- Labels are also multi-valued for a release, so we roll them up for simple consumption.
  -- DISTINCT avoids duplicates when a label appears more than once (e.g., multiple catalog numbers).
  SELECT
    s.release_group_id,
    string_agg(DISTINCT l.name::text, ' | '::text) AS label_names,
    string_agg(DISTINCT l.gid::text, ' | '::text) AS label_mbids
  FROM rg_set s
  LEFT JOIN canonical_release_any cr ON cr.release_group_id = s.release_group_id
  LEFT JOIN musicbrainz.release_label rl ON rl.release = cr.release_id
  LEFT JOIN musicbrainz.label l ON l.id = rl.label
  GROUP BY s.release_group_id
),
label_tag_counts AS (
  -- Label tags require an intermediate aggregation: multiple labels per release, each with tag counts.
  -- We sum tag counts across all labels attached to the canonical release.
  SELECT
    s.release_group_id,
    t.name AS tag_name,
    sum(lt.count) AS tag_count
  FROM rg_set s
  LEFT JOIN canonical_release_any cr ON cr.release_group_id = s.release_group_id
  LEFT JOIN musicbrainz.release_label rl ON rl.release = cr.release_id
  LEFT JOIN musicbrainz.label_tag lt ON lt.label = rl.label
  LEFT JOIN musicbrainz.tag t ON t.id = lt.tag
  GROUP BY s.release_group_id, t.name
),
label_tags AS (
  -- Roll up the label tag counts into both text (same pattern as release_group_tags).
  SELECT
    release_group_id,
    string_agg(tag_name::text, ' | '::text ORDER BY tag_count DESC, tag_name::text) AS label_tags_text
  FROM label_tag_counts
  WHERE tag_name IS NOT NULL
  GROUP BY release_group_id
),
rg_meta AS (
  -- Release-group-level rating metadata (if present).
  -- LEFT JOIN behavior: missing meta rows produce NULLs (we never drop matches because of missing enrichment).
  SELECT
    s.release_group_id,
    rgm.rating,
    rgm.rating_count
  FROM rg_set s
  LEFT JOIN musicbrainz.release_group_meta rgm ON rgm.id = s.release_group_id
),
release_meta AS (
  -- Release-level metadata for the canonical release (date added, info_url, ASIN, cover-art indicator).
  SELECT
    s.release_group_id,
    rm.date_added,
    rm.info_url,
    rm.amazon_asin,
    rm.cover_art_presence
  FROM rg_set s
  LEFT JOIN canonical_release_any cr ON cr.release_group_id = s.release_group_id
  LEFT JOIN musicbrainz.release_meta rm ON rm.id = cr.release_id
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  -- Film attributes (TMDB staging), duplicated across each linked soundtrack row for easy analysis.
  f.tmdb_id,
  f.film_title,
  f.film_adult,
  f.film_runtime_min,
  f.film_genres,
  f.film_rating,
  f.film_vote_count,
  f.film_mpaa_rating,
  f.film_original_title,
  f.film_language_name,
  f.film_imdb_id,
  f.film_wikidata_id,
  f.film_countries,
  f.film_year,
  f.film_release_date,
  f.film_popularity,
  f.film_budget,
  f.film_revenue,
  f.film_studios,
  f.film_director,
  f.film_soundtrack_composer_raw,
  f.film_top_cast,
  f.film_keywords,
  f.film_ingested_at,
  -- Link/match metadata (how we connected this film to this release_group).
  m.match_method,
  m.soundtrack_type,
  m.notes,
  m.matched_at,
  -- Release-group identifiers + human-readable album title.
  rg.id AS release_group_id,
  rg.gid AS release_group_mbid,
  rg.name AS album_title,
  rgpt.name AS rg_primary_type,
  rgst.rg_secondary_types,
  -- Canonical release identifiers + release-level metadata.
  cr.release_id,
  r.gid AS release_mbid,
  r.name AS release_title,
  rs.name AS release_status,
  rp.name AS release_packaging,
  r.barcode,
  lang.name AS release_language,
  scr.name AS release_script,
  r.comment AS release_comment,
  -- US release date enrichment (computed from release_event rows; never used for filtering in the MV).
  d.album_us_release_date,
  d.album_us_release_year,
  d.album_us_release_month_min_observed,
  d.album_us_release_day_min_observed,
  d.us_date_has_missing_month,
  d.us_date_has_missing_day,
  a.us_any_event_missing_month,
  a.us_any_event_missing_day,
  -- Tags / labels / rating / misc meta (rolled up for easy downstream analysis).
  rgt.rg_tags_text,
  rt.release_tags_text,
  rl.label_names,
  rl.label_mbids,
  lt.label_tags_text,
  rgm.rating AS rg_rating,
  rgm.rating_count AS rg_rating_count,
  rm.date_added AS release_meta_date_added,
  rm.info_url AS release_meta_info_url,
  rm.amazon_asin AS release_meta_amazon_asin,
  rm.cover_art_presence AS release_meta_cover_art_presence
FROM matches_all m
JOIN tmdb_films f
  ON f.tmdb_id = m.tmdb_id -- Start from match rows, then attach film metadata.
JOIN musicbrainz.release_group rg
  ON rg.id = m.release_group_id -- Attach album-level metadata (release_group).
LEFT JOIN canonical_release_any cr
  ON cr.release_group_id = rg.id -- Canonical release chosen per release_group.
LEFT JOIN musicbrainz.release r
  ON r.id = cr.release_id -- Release-level metadata for the canonical release (nullable).
LEFT JOIN musicbrainz.release_group_primary_type rgpt
  ON rgpt.id = rg.type -- Primary type label (e.g., Album, Single) at the release_group level.
LEFT JOIN musicbrainz.release_status rs
  ON rs.id = r.status
LEFT JOIN musicbrainz.release_packaging rp
  ON rp.id = r.packaging
LEFT JOIN musicbrainz.language lang
  ON lang.id = r.language
LEFT JOIN musicbrainz.script scr
  ON scr.id = r.script
LEFT JOIN rg_first_us_date d
  ON d.release_group_id = rg.id -- Earliest US release date (best-effort).
LEFT JOIN rg_us_any_missing a
  ON a.release_group_id = rg.id -- Any missing month/day among US events.
LEFT JOIN release_group_secondary_types rgst
  ON rgst.release_group_id = rg.id
LEFT JOIN release_group_tags rgt
  ON rgt.release_group_id = rg.id
LEFT JOIN release_tags rt
  ON rt.release_group_id = rg.id
LEFT JOIN release_labels rl
  ON rl.release_group_id = rg.id
LEFT JOIN label_tags lt
  ON lt.release_group_id = rg.id
LEFT JOIN rg_meta rgm
  ON rgm.release_group_id = rg.id
LEFT JOIN release_meta rm
  ON rm.release_group_id = rg.id;


/* =============================================================================
DATA DICTIONARY: musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025
--------------------------------------------------------------------------------
NOTE:
  This dictionary describes the columns produced by the MV *as currently written*.
  If you add/remove columns in the SELECT list, update this section so it stays
  trustworthy for future readers.

KEYS / IDENTIFIERS
  tmdb_id                     bigint      TMDB film identifier.
  release_group_id            int         MusicBrainz internal PK for release_group.
  release_group_mbid          uuid/txt    MusicBrainz release_group GID (MBID).
  release_id                  int         “Canonical” release id chosen per release_group (min(release.id)).
  release_mbid                uuid/txt    MusicBrainz release GID (MBID).

TMDB FILM ATTRIBUTES (from tmdb_movies_2015_2025_staging; prefixed film_)
  film_title                  text        TMDB film title.
  film_adult                  boolean     TMDB adult flag.
  film_runtime_min            int         Runtime in minutes.
  film_genres                 text        Genres (as ingested; list-like text).
  film_rating                 real        TMDB rating (0–10).
  film_vote_count             int         TMDB vote count (NULL treated as 0).
  film_mpaa_rating            text        MPAA rating (as ingested).
  film_original_title         text        Original title (as ingested).
  film_language_name          text        Language name (as ingested).
  film_imdb_id                text        IMDb title id (tt#######), trimmed; NULL if blank.
  film_wikidata_id            text        Wikidata entity id (Q####), trimmed; NULL if blank.
  film_countries              text        Countries (as ingested; list-like text).
  film_year                   int         Release year (as ingested).
  film_release_date           text        Release date (as ingested; typically YYYY-MM-DD).
  film_popularity             numeric     TMDB popularity score.
  film_budget                 bigint      Budget (as ingested).
  film_revenue                bigint      Revenue (as ingested).
  film_studios                text        Studios (as ingested; list-like text).
  film_director               text        Director (as ingested).
  film_soundtrack_composer_raw text       Raw “Soundtrack/Composer” field (often sparse/inconsistent).
  film_top_cast               text        Top cast (as ingested; list-like text).
  film_keywords               text        Keywords (as ingested; list-like text).
  film_ingested_at            timestamptz Timestamp when the TMDB row was loaded.

MATCH / LINK METADATA (from matches + candidates)
  match_method                text        How the film↔album link was obtained (e.g., imdb_exact, title_contains_strict).
  soundtrack_type             text        Coarse heuristic type (score/songs/single/inspired_by/unknown).
  notes                       text        Candidate-only notes (NULL for final matches).
  matched_at                  timestamptz Timestamp when the match/candidate row was created.

RELEASE GROUP ATTRIBUTES
  album_title                 text        release_group.name (album-level title).
  rg_primary_type             text        release_group primary type label (from release_group_primary_type).
  rg_secondary_types          text        ' | '-delimited release_group secondary types (sorted).

CANONICAL RELEASE ATTRIBUTES (release_id = min(release.id) per release_group)
  release_title               text        release.name.
  release_status              text        Release status label.
  release_packaging           text        Packaging label.
  barcode                     text        release.barcode.
  release_language            text        Language name.
  release_script              text        Script name.
  release_comment             text        release.comment (disambiguation/comment field).

US RELEASE DATE ENRICHMENT (from release_event; informational only, not a filter)
  album_us_release_date               date     Earliest US release date observed (month/day may default to 1).
  album_us_release_year               int      US release year for the chosen earliest US event.
  album_us_release_month_min_observed int      Month from the earliest US event row (nullable).
  album_us_release_day_min_observed   int      Day from the earliest US event row (nullable).
  us_date_has_missing_month           boolean  True if earliest US event row has NULL month.
  us_date_has_missing_day             boolean  True if earliest US event row has NULL day.
  us_any_event_missing_month          boolean  True if ANY US event row for the RG has NULL month.
  us_any_event_missing_day            boolean  True if ANY US event row for the RG has NULL day.

TAGS / LABELS / RATINGS / META (rolled up for convenience)
  rg_tags_text                text        Release_group tags concatenated with ' | ' (sorted by tag count desc then name).
  release_tags_text           text        Canonical release tags concatenated with ' | '.
  label_names                 text        Distinct label names on canonical release concatenated with ' | '.
  label_mbids                 text        Distinct label MBIDs on canonical release concatenated with ' | '.
  label_tags_text             text        Aggregated label tags across canonical-release labels, ' | '-delimited.
  rg_rating                   numeric     release_group_meta.rating.
  rg_rating_count             int         release_group_meta.rating_count.
  release_meta_date_added     timestamptz release_meta.date_added for canonical release.
  release_meta_info_url       text        release_meta.info_url for canonical release.
  release_meta_amazon_asin    text        release_meta.amazon_asin for canonical release.
  release_meta_cover_art_presence int/boolean release_meta.cover_art_presence (presence indicator as stored).
============================================================================= */

COMMIT;

select count(*) from musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025;
-- 5328 albums --> 5209 albums after us release date fix

/* ============================================================================
4.2 FILM → SOUNDTRACK MAPPING VALIDATION AND COUNTS (DISTRIBUTION)
-------------------------------------------------------------------------------
GOAL:
  Summarize how many soundtrack matches each film has, then roll that up into a
  distribution so we can report things like:
    "89 films have 5 associated soundtracks, 53 have 4 associated soundtracks, ..."

DEFINITION OF “MAPPING” USED HERE:
  A “mapping” = a row in mv_tmdb_soundtrack_album_spine_2015_2025 where
  soundtrack_type IS NOT NULL (i.e., a FINAL match from tmdb_mb_soundtrack_matches_2015_2025).
  Candidate-only rows (soundtrack_type IS NULL) are excluded.

OUTPUT:
  One row per soundtrack-count bucket:
    - matched_soundtrack_rows: number of matched soundtrack rows per film
    - film_ct:                how many films fall into that bucket

NOTES:
  - This counts matched *rows* (not distinct release_group_id). If you prefer
    distinct albums per film, swap COUNT(*) for COUNT(DISTINCT release_group_id).
  - Films with zero matched soundtracks are excluded by default. To include them,
    LEFT JOIN from the TMDB film list and COALESCE the count to 0.
============================================================================ */

WITH film_counts AS (
  SELECT
    tmdb_id,
    COUNT(*) FILTER (WHERE soundtrack_type IS NOT NULL) AS matched_soundtrack_rows
  FROM musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025
  GROUP BY tmdb_id
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  matched_soundtrack_rows,
  COUNT(*) AS film_ct
FROM film_counts
WHERE matched_soundtrack_rows > 0
GROUP BY matched_soundtrack_rows
ORDER BY matched_soundtrack_rows DESC;

/* 	6	1
	5	3
	4	9
	3	48
	2	293
	1	4422
	Looks like we have some cleanup to do
*/

-- Show the important mapping columns in the MV
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  tmdb_id,
  film_title,
  film_year,
  COUNT(*) FILTER (WHERE soundtrack_type IS NOT NULL)                AS matched_soundtrack_rows,
  COUNT(*) FILTER (WHERE soundtrack_type IS NULL)                    AS candidate_rows,
  COUNT(*) FILTER (WHERE match_method = 'imdb_exact')                AS imdb_exact_rows,
  COUNT(*) FILTER (WHERE match_method = 'title_contains_strict')     AS title_contains_strict_rows,
  COUNT(DISTINCT release_group_id) FILTER (WHERE soundtrack_type IS NOT NULL) AS matched_release_groups,
  COUNT(DISTINCT release_group_id) FILTER (WHERE soundtrack_type IS NULL)     AS candidate_release_groups
FROM musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025
GROUP BY tmdb_id, film_title, film_year
ORDER BY matched_soundtrack_rows DESC, film_year DESC, film_title
LIMIT 5000;

st.markdown("""
### After the SQL\\.\\.\\.
""")

st.markdown("""
Please note that after running this SQL on the MusicBrainz database, we exported the MATERIALIZED VIEW 2\\.1\\.musicbrainz\\.mv\\_tmdb\\_soundtrack\\_album\\_spine\\_2015\\_2025 into \\.csv to do further exploration and cleaning within Python\\.
""")
