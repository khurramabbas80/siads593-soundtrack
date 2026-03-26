import streamlit as st
import os, sys

st.set_page_config(page_title="2.2.Build_artist_track_wide_MVs.sql", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
# Documentation\\-Only SQL Artifact
""")

st.markdown("""
This notebook contains a verbatim copy of the SQL script \\`Build\\_artist\\_track\\_wide\\_MVs\\.sql\\` for transparency and review\\.
""")

st.markdown("""
It is provided for reproducibility documentation only and should not be executed within the notebook runtime\\.
""")

st.markdown("""
Materialized view creation must occur in the canonical Postgres database environment to preserve schema integrity and QA controls then exported in a CSV, which is then read in by notebooks 3\\.1 and 3\\.2\\.
""")

st.markdown("""
### CELL 1 — Reset / drop prior MVs
""")

st.markdown("""
Clears any previously\\-built materialized views that this script regenerates \\(artist spine, track spine, wide track MV, and related bridge surfaces\\)\\.
""")

DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_wide_track_2015_2025;
DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025;
DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025;

st.markdown("""
### CELL 2 — Artist spine MV: soundtrack\\-universe artist lookup
""")

st.markdown("""
Builds a one\\-row\\-per\\-artist table for artists credited on soundtrack release groups in the film→album spine, including core attributes plus rolled\\-up aliases, tags, and identifiers \\(ISNI/IPI\\), along with within\\-spine counts\\.
""")

/**********
* ARTIST
**********/

/* =============================================================================
MATERIALIZED VIEW: musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025
--------------------------------------------------------------------------------
PURPOSE:
  Create an artist-level lookup table for the soundtrack universe defined by
  mv_tmdb_soundtrack_album_spine_2015_2025.

  This MV answers:
    “Which MusicBrainz artists appear on soundtrack release_groups in our film→album spine,
     and what are their core attributes, aliases, tags, and IDs?”

DATA SOURCE:
  - musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025 (defines the release_group universe)
  - MusicBrainz core tables for artist attributes, aliases, tags, identifiers (ISNI/IPI)

GRAIN:
  One row per artist_id.

IMPORTANT NOTES:
  - Artist membership is derived from release_group.artist_credit (album-level credit).
  - Counts (film_ct_in_spine, soundtrack_release_group_ct_in_spine) are *within the spine only*.
  - This MV is an enrichment surface; it must not filter the spine universe.
============================================================================= */
DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025;

CREATE MATERIALIZED VIEW musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025 AS
WITH spine_rg AS (
  -- Distinct album universe (tmdb_id × release_group_id) from the album spine.
  SELECT DISTINCT s.tmdb_id, s.release_group_id
  FROM musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025 s
),
rg_artist_credits AS (
  -- Pull artist_credit_id from each release_group so we can enumerate credited artists.
  SELECT DISTINCT sr.tmdb_id,
         sr.release_group_id,
         rg.artist_credit AS artist_credit_id
  FROM spine_rg sr
  JOIN musicbrainz.release_group rg
    ON rg.id = sr.release_group_id
  WHERE rg.artist_credit IS NOT NULL
),
spine_artists AS (
  -- Expand each artist_credit into the individual artists credited on that album.
  SELECT DISTINCT rac.tmdb_id,
         rac.release_group_id,
         acn.artist AS artist_id
  FROM rg_artist_credits rac
  JOIN musicbrainz.artist_credit_name acn
    ON acn.artist_credit = rac.artist_credit_id
),
artist_rollups AS (
  -- Roll up “how many films/albums does this artist appear on (within our spine)?”
  SELECT sa.artist_id,
         COUNT(DISTINCT sa.tmdb_id) AS film_ct_in_spine,
         COUNT(DISTINCT sa.release_group_id) AS soundtrack_release_group_ct_in_spine
  FROM spine_artists sa
  GROUP BY sa.artist_id
),
aliases AS (
  -- Deduplicate aliases then roll them up into both text for easy downstream use.
  WITH dedup AS (
    SELECT aa.artist,
           aa.name::text AS alias_name
    FROM musicbrainz.artist_alias aa
    WHERE aa.name IS NOT NULL
    GROUP BY aa.artist, aa.name::text
  )
  SELECT d.artist AS artist_id,
         string_agg(d.alias_name, ' | ' ORDER BY d.alias_name) AS aliases_text
  FROM dedup d
  GROUP BY d.artist
),
artist_tags AS (
  -- Artist tags are community-entered. We keep both a readable string.
  SELECT a.id AS artist_id,
         string_agg(t.name::text, ' | '::text ORDER BY at.count DESC, t.name::text) AS artist_tags_text
  FROM musicbrainz.artist a
  LEFT JOIN musicbrainz.artist_tag at ON at.artist = a.id
  LEFT JOIN musicbrainz.tag t ON t.id = at.tag
  GROUP BY a.id
),
identifiers AS (
  -- ISNI and IPI live in separate tables; we aggregate each, then FULL JOIN so we don’t lose artists.
  WITH isni AS (
    SELECT artist AS artist_id,
           string_agg(DISTINCT isni::text, ' | ' ORDER BY isni::text) AS isni_text
    FROM musicbrainz.artist_isni
    WHERE isni IS NOT NULL
    GROUP BY artist
  ),
  ipi AS (
    SELECT artist AS artist_id,
           string_agg(DISTINCT ipi::text, ' | ' ORDER BY ipi::text) AS ipi_text
    FROM musicbrainz.artist_ipi
    WHERE ipi IS NOT NULL
    GROUP BY artist
  )
  SELECT COALESCE(i.artist_id, p.artist_id) AS artist_id,
         i.isni_text,
         p.ipi_text
  FROM isni i
  FULL JOIN ipi p ON p.artist_id = i.artist_id
)
SELECT ar.artist_id,
       a.gid AS artist_mbid,
       a.name,
       a.sort_name,
       atype.name AS artist_type,
       g.name AS gender,
       a.comment AS artist_comment,
       a.begin_date_year,
       a.begin_date_month,
       a.begin_date_day,
       a.end_date_year,
       a.end_date_month,
       a.end_date_day,
       area.name AS area_name,
       area.gid AS area_mbid,
       begin_area.name AS begin_area_name,
       begin_area.gid AS begin_area_mbid,
       end_area.name AS end_area_name,
       end_area.gid AS end_area_mbid,
       al.aliases_text,
       tg.artist_tags_text,
       ids.isni_text,
       ids.ipi_text,
       ar.film_ct_in_spine,
       ar.soundtrack_release_group_ct_in_spine
FROM artist_rollups ar
JOIN musicbrainz.artist a ON a.id = ar.artist_id
LEFT JOIN musicbrainz.artist_type atype ON atype.id = a.type
LEFT JOIN musicbrainz.gender g ON g.id = a.gender
LEFT JOIN musicbrainz.area area ON area.id = a.area
LEFT JOIN musicbrainz.area begin_area ON begin_area.id = a.begin_area
LEFT JOIN musicbrainz.area end_area ON end_area.id = a.end_area
LEFT JOIN aliases al ON al.artist_id = a.id
LEFT JOIN artist_tags tg ON tg.artist_id = a.id
LEFT JOIN identifiers ids ON ids.artist_id = a.id;

COMMIT;

st.markdown("""
### CELL 3 — Artist spine QA queries \\(counts \\+ “top” views\\)
""")

st.markdown("""
Provides sanity checks and quick inspection queries to validate the artist spine: total artist counts, top artists by album/film coverage, alias/tag previews, and sample row browsing\\.
""")

select count(*) from musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025
-- 2480 Artists --> 2430 Artist

SELECT *
FROM musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025
ORDER BY soundtrack_release_group_ct_in_spine DESC, film_ct_in_spine DESC, name
LIMIT 50;

SELECT artist_id,
       name,
       artist_type,
       gender,
       film_ct_in_spine,
       soundtrack_release_group_ct_in_spine,
       left(coalesce(artist_tags_text, ''), 200) AS artist_tags_preview,
       left(coalesce(aliases_text, ''), 200) AS aliases_preview
FROM musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025
ORDER BY film_ct_in_spine DESC, soundtrack_release_group_ct_in_spine DESC, name
LIMIT 100;

st.markdown("""
### CELL 4 — Artist↔album connectivity inspection
""")

st.markdown("""
Walkthrough queries to show which albums an artist is connected to \\(via album\\-level credits\\) and to display film \\+ album \\+ credited artist strings for human validation\\.
""")

-- show the albums this artist is connected to
WITH spine AS (
  SELECT DISTINCT tmdb_id, release_group_id
  FROM musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025
),
artist_to_rg AS (
  SELECT DISTINCT acn.artist AS artist_id,
         s.release_group_id
  FROM spine s
  JOIN musicbrainz.release_group rg ON rg.id = s.release_group_id
  JOIN musicbrainz.artist_credit_name acn ON acn.artist_credit = rg.artist_credit
)
SELECT a.artist_id,
       a.name AS artist_name,
       COUNT(DISTINCT atr.release_group_id) AS album_ct_in_spine,
       string_agg(DISTINCT rg.name, ' | ' ORDER BY rg.name) AS album_titles_in_spine
FROM musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025 a
JOIN artist_to_rg atr ON atr.artist_id = a.artist_id
JOIN musicbrainz.release_group rg ON rg.id = atr.release_group_id
GROUP BY a.artist_id, a.name
ORDER BY album_ct_in_spine DESC, a.name
LIMIT 50;

-- Artist, film and album
SELECT s.tmdb_id,
       s.film_title,
       s.film_year,
       s.release_group_id,
       s.album_title,
       s.match_method,
       string_agg(acn.name, ' + ' ORDER BY acn.position) AS credited_artists
FROM musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025 s
JOIN musicbrainz.release_group rg ON rg.id = s.release_group_id
JOIN musicbrainz.artist_credit_name acn ON acn.artist_credit = rg.artist_credit
GROUP BY s.tmdb_id, s.film_title, s.film_year, s.release_group_id, s.album_title, s.match_method
ORDER BY s.film_year DESC NULLS LAST, s.film_title, s.album_title
LIMIT 200;

st.markdown("""
### CELL 5 — Track spine MV: enumerate mediums/tracks/recordings
""")

st.markdown("""
Creates the track\\-grained spine for each soundtrack album in the film→album spine: medium \\+ track structure plus recording\\-level enrichment \\(first release date parts, formatted artist credit, ISRC aggregation, tags, work links, composer/lyricist extraction when available\\)\\.
""")

/*************
 * TRACK
 *************/

/* =============================================================================
MATERIALIZED VIEW: musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025
--------------------------------------------------------------------------------
PURPOSE:
  Enumerate mediums/tracks/recordings for each soundtrack album in the film→album
  spine (mv_tmdb_soundtrack_album_spine_2015_2025), and enrich tracks with:
    - recording IDs + titles + first-release dates (if present)
    - recording artist credit (formatted)
    - ISRCs (deduped + aggregated)
    - recording tags
    - best-effort work links + composer/lyricist names (when MB relationships exist)

DATA SOURCE:
  - musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025 (album universe + canonical release_id)
  - MusicBrainz core tables: medium, track, recording, isrc, tags, work relationships

GRAIN:
  One row per:
    (tmdb_id, release_group_id, match_method, release_id, medium_id, track_id)

NON-NEGOTIABLE RULE:
  - Do NOT filter out match/candidate rows except where required to enumerate tracks:
      we must require release_id IS NOT NULL to join to medium/track.
    (If a spine row has no canonical release_id, it simply cannot enumerate tracks.)

NOTES:
  - Work credits are best-effort. Missing recording→work or work→artist relationships
    are common in community-entered metadata and should be treated as “unknown”, not “false”.
============================================================================= */
DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025;

CREATE MATERIALIZED VIEW musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025 AS
WITH spine AS (
  -- Minimal set of columns needed from the album spine to enumerate tracks.
  SELECT
    s.tmdb_id,
    s.film_title,
    s.release_group_id,
    s.release_id,
    s.match_method,
    s.album_us_release_date,
    s.us_date_has_missing_month,
    s.us_date_has_missing_day
  FROM musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025 s
  WHERE s.release_id IS NOT NULL -- Required to enumerate mediums/tracks (medium.release → release.id).
),
spine_rg AS (
  -- DISTINCT prevents accidental row multiplication if the album spine contains duplicate rg edges.
  SELECT DISTINCT
    tmdb_id, film_title, release_group_id, release_id, match_method,
    album_us_release_date, us_date_has_missing_month, us_date_has_missing_day
  FROM spine
),
mediums AS (
  -- Enumerate the medium(s) (disc containers) within the canonical release.
  SELECT
    sr.*,
    m.id AS medium_id,
    m.position AS disc_number,
    mf.name AS medium_format
  FROM spine_rg sr
  JOIN musicbrainz.medium m ON m.release = sr.release_id
  LEFT JOIN musicbrainz.medium_format mf ON mf.id = m.format
),
tracks AS (
  -- Enumerate tracks on each medium and capture the recording_id they point to.
  SELECT
    md.*,
    t.id AS track_id,
    t.position AS track_number,
    t.name AS track_title,
    t.length AS track_length_ms,
    t.recording AS recording_id
  FROM mediums md
  JOIN musicbrainz.track t ON t.medium = md.medium_id
),
recording_core AS (
  -- Recording-level fields (shared across many tracks/releases).
  SELECT
    r.id AS recording_id,
    r.gid AS recording_mbid,
    r.name AS recording_title,
    r.length AS recording_length_ms,
    r.artist_credit AS recording_artist_credit_id,
    rfrd.year::int AS recording_first_release_year,
    rfrd.month::int AS recording_first_release_month,
    rfrd.day::int AS recording_first_release_day
  FROM musicbrainz.recording r
  LEFT JOIN musicbrainz.recording_first_release_date rfrd ON rfrd.recording = r.id
),
recording_artist_credit AS (
  -- Format the recording artist credit into a single string (names + join phrases in order).
  SELECT
    acn.artist_credit AS artist_credit_id,
    string_agg(
      acn.name::text || COALESCE(acn.join_phrase::text, ''),
      '' ORDER BY acn.position
    ) AS recording_artist_credit
  FROM musicbrainz.artist_credit_name acn
  GROUP BY acn.artist_credit
),
isrc_dedup AS (
  -- Deduplicate ISRCs per recording (same ISRC can appear multiple times).
  SELECT
    i.recording AS recording_id,
    i.isrc::text AS isrc_value
  FROM musicbrainz.isrc i
  WHERE i.isrc IS NOT NULL
  GROUP BY i.recording, i.isrc::text
),
isrc_agg AS (
  -- Aggregate ISRCs into text.
  SELECT
    d.recording_id,
    string_agg(d.isrc_value, ' | ' ORDER BY d.isrc_value) AS isrcs_text
  FROM isrc_dedup d
  GROUP BY d.recording_id
),
recording_tag_agg AS (
  -- Aggregate recording tags (community-entered).
  SELECT
    rt.recording AS recording_id,
    string_agg(t.name::text, ' | ' ORDER BY rt.count DESC, t.name::text) AS recording_tags_text
  FROM musicbrainz.recording_tag rt
  JOIN musicbrainz.tag t ON t.id = rt.tag
  GROUP BY rt.recording
),
recording_work_dedup AS (
  -- Link recordings to works (best-effort: many recordings have no work relationships).
  SELECT
    lrw.entity0 AS recording_id,
    lrw.entity1 AS work_id
  FROM musicbrainz.l_recording_work lrw
  GROUP BY lrw.entity0, lrw.entity1
),
work_core AS (
  SELECT w.id AS work_id, w.name AS work_title
  FROM musicbrainz.work w
),
work_links AS (
  -- Roll up work IDs and titles per recording for quick inspection.
  SELECT
    rwd.recording_id,
    string_agg(rwd.work_id::text, ' | ' ORDER BY rwd.work_id::text) AS work_ids_text,
    string_agg(wc.work_title::text, ' | ' ORDER BY wc.work_title::text) AS work_titles_text
  FROM recording_work_dedup rwd
  LEFT JOIN work_core wc ON wc.work_id = rwd.work_id
  GROUP BY rwd.recording_id
),
work_artist_rels AS (
  -- Pull composer/lyricist from work↔artist relationships (best-effort; depends on link_type naming).
  SELECT
    rwd.recording_id,
    rwd.work_id,
    lower(lt.name) AS rel_type,
    a.name::text AS artist_name
  FROM recording_work_dedup rwd
  JOIN musicbrainz.l_artist_work law ON law.entity1 = rwd.work_id
  JOIN musicbrainz.link l ON l.id = law.link
  JOIN musicbrainz.link_type lt ON lt.id = l.link_type
  JOIN musicbrainz.artist a ON a.id = law.entity0
  WHERE lower(lt.name) IN ('composer','lyricist')
),
composer_agg AS (
  SELECT
    recording_id,
    string_agg(DISTINCT artist_name, ' | ' ORDER BY artist_name) AS composer_names_text
  FROM work_artist_rels
  WHERE rel_type = 'composer'
  GROUP BY recording_id
),
lyricist_agg AS (
  SELECT
    recording_id,
    string_agg(DISTINCT artist_name, ' | ' ORDER BY artist_name) AS lyricist_names_text
  FROM work_artist_rels
  WHERE rel_type = 'lyricist'
  GROUP BY recording_id
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  -- Spine keys/context
  tr.tmdb_id,
  tr.film_title,
  tr.release_group_id,
  tr.release_id,
  tr.match_method,
  tr.album_us_release_date,
  tr.us_date_has_missing_month,
  tr.us_date_has_missing_day,
  -- Medium/track identifiers + ordering
  tr.medium_id,
  tr.disc_number,
  tr.medium_format,
  tr.track_id,
  tr.track_number,
  tr.track_title,
  tr.track_length_ms,
  -- Recording identifiers + attributes
  tr.recording_id,
  rc.recording_mbid,
  rc.recording_title,
  rc.recording_length_ms,
  rac.recording_artist_credit,
  rc.recording_first_release_year,
  rc.recording_first_release_month,
  rc.recording_first_release_day,
  -- Enrichment: ISRCs + tags + works + composer/lyricist
  ia.isrcs_text,
  rta.recording_tags_text,
  wl.work_ids_text,
  wl.work_titles_text,
  ca.composer_names_text,
  la.lyricist_names_text
FROM tracks tr
LEFT JOIN recording_core rc ON rc.recording_id = tr.recording_id
LEFT JOIN recording_artist_credit rac ON rac.artist_credit_id = rc.recording_artist_credit_id
LEFT JOIN isrc_agg ia ON ia.recording_id = tr.recording_id
LEFT JOIN recording_tag_agg rta ON rta.recording_id = tr.recording_id
LEFT JOIN work_links wl ON wl.recording_id = tr.recording_id
LEFT JOIN composer_agg ca ON ca.recording_id = tr.recording_id
LEFT JOIN lyricist_agg la ON la.recording_id = tr.recording_id;
--1 min 41 sec

COMMIT;

st.markdown("""
### CELL 6 — Track spine data dictionary \\+ counts
""")

st.markdown("""
Documents the output fields and provides a baseline row\\-count validation for the track spine\\.
""")

/* Data dictionary (output columns)
  Film/album spine context (from mv_tmdb_soundtrack_album_spine_2015_2025)
    tmdb_id                       bigint     TMDB film identifier.
    film_title                    text       TMDB film title.
    release_group_id              int        Release group PK (album container).
    release_id                    int        Canonical release PK from the album spine.
    match_method                  text       Match method that linked film↔album (e.g., imdb_exact, title_contains_strict).
    match_score                   numeric    Match confidence score.
    album_us_release_date         date       Earliest US release date enrichment for the release group (nullable).
    us_date_has_missing_month     boolean    TRUE if chosen earliest US date row had NULL month.
    us_date_has_missing_day       boolean    TRUE if chosen earliest US date row had NULL day.
  Medium / track identifiers
    medium_id                     int        Medium PK (disc/container within the release).
    track_id                      int        Track PK (position on a specific medium).
    recording_id                  int        Recording PK (underlying audio entity).
    recording_mbid                uuid/txt    Recording GID (MBID).
  Medium / track attributes
    disc_number                   int        Medium position within the release (e.g., 1, 2).
    medium_format                 text       Medium format name (e.g., CD, Digital Media), if present.
    track_number                  int        Track position within the medium.
    track_title                   text       Track title as printed for this track on this medium.
    track_length_ms               int        Track length in milliseconds (track-specific; may be null).
  Recording attributes
    recording_title               text       Recording title (canonical name for the recording entity).
    recording_length_ms           int        Recording length in milliseconds (recording-level; may be null).
    recording_artist_credit       text       Formatted artist credit for the recording (names + join phrases).
  ISRC enrichments (per recording)
    isrcs_text                    text       Distinct ISRCs concatenated with ' | ' (may be null).
  Recording first release date parts (per recording)
    recording_first_release_year  int        First release year of the recording (if available).
    recording_first_release_month int        First release month of the recording (if available).
    recording_first_release_day   int        First release day of the recording (if available).
  Recording tags (per recording; community-driven)
    recording_tags_text           text       Tags as ' | '-delimited text ordered by count desc.
  Work credits (best-effort; via recording→work and work→artist relationships)
    work_ids_text                 text       Distinct work IDs linked to the recording, ' | '-delimited (may be null).
    work_titles_text              text       Distinct work titles linked to the recording, ' | '-delimited (may be null).
    composer_names_text           text       Distinct composer artist names across linked works, ' | '-delimited (may be null).
    lyricist_names_text           text       Distinct lyricist artist names across linked works, ' | '-delimited (may be null).
*/

select count(*) from musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025
-- 88030 Tracks --> 85842 Tracks

st.markdown("""
### CELL 7 — Track spine QA suite \\(spot checks \\+ distributions\\)
""")

st.markdown("""
A set of human\\-readable queries to sanity\\-check the track spine joins and enrichments \\(sample rows, top track artists, composer/lyricist coverage, album enrichment completeness, and “box set / collection” outlier detection\\)\\.
""")

/*
 * Query 1 — “Row-level spot check (Film → Album → Track)”
 *
 * Purpose: human-readable sample rows to sanity-check joins and core fields (titles, ordering, composer/lyricist presence, ISRC/tags previews), with both album-level and track-level artist context if included.
 * Use to eyeball whether the MV “looks right” for real records.
 */

SELECT t.film_year,
       t.film_title,
       t.album_title,
       aa.album_artist_name,
       aa.album_artist_type,
       tr.disc_number,
       tr.track_number,
       tr.track_title,
       tr.recording_title,
       tr.recording_artist_credit AS track_artist_credit,
       tr.composer_names_text AS composer,
       tr.lyricist_names_text AS lyricist,
       tr.recording_length_ms,
       tr.track_length_ms,
       left(coalesce(tr.isrcs_text, ''), 80) AS isrcs_preview,
       left(coalesce(tr.recording_tags_text, ''), 120) AS recording_tags_preview,
       t.match_method
FROM musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025 tr
JOIN musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025 t
  ON t.tmdb_id = tr.tmdb_id
 AND t.release_group_id = tr.release_group_id
 AND t.match_method = tr.match_method
LEFT JOIN LATERAL (
  SELECT string_agg(a.name, ' | ' ORDER BY acn.position) AS album_artist_name,
         string_agg(COALESCE(atype.name, ''), ' | ' ORDER BY acn.position) AS album_artist_type
  FROM musicbrainz.release_group rg
  JOIN musicbrainz.artist_credit_name acn
    ON acn.artist_credit = rg.artist_credit
  JOIN musicbrainz.artist a
    ON a.id = acn.artist
  LEFT JOIN musicbrainz.artist_type atype
    ON atype.id = a.type
  WHERE rg.id = t.release_group_id
) aa ON true
ORDER BY t.film_year DESC NULLS LAST,
         t.film_title,
         t.album_title,
         tr.disc_number,
         tr.track_number
LIMIT 500;

/*
 * Query 2 — “Top track artists by volume”
 *
 * Purpose: shows which track-level artist credits dominate the spine (helps catch explosions/duplication or unexpected credit formatting).
 * Use as a quick QA that you aren’t accidentally multiplying rows or pulling in weird “Various Artists” artifacts.
*/
SELECT tr.recording_artist_credit AS artist,
       COUNT(*) AS spine_track_rows,
       COUNT(DISTINCT tr.recording_id) AS distinct_recordings,
       COUNT(DISTINCT tr.release_group_id) AS distinct_albums,
       SUM(CASE WHEN tr.composer_names_text IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_composer,
       SUM(CASE WHEN tr.lyricist_names_text IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_lyricist
FROM musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025 tr
GROUP BY tr.recording_artist_credit
ORDER BY spine_track_rows DESC
LIMIT 50;

/**
 * Query 3 — “Composer/Lyricist coverage by track artist”
 *
 * Purpose: confirms your work→composer/lyricist extraction is populating, and which artists tend to have those fields.
 * Use to verify composer/lyricist isn’t always null and spot-check the patterns.
 */
SELECT tr.recording_artist_credit AS artist,
       tr.composer_names_text AS composer,
       tr.lyricist_names_text AS lyricist,
       COUNT(*) AS rows
FROM musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025 tr
WHERE tr.composer_names_text IS NOT NULL OR tr.lyricist_names_text IS NOT NULL
GROUP BY tr.recording_artist_credit, tr.composer_names_text, tr.lyricist_names_text
ORDER BY rows DESC
LIMIT 100;

/**
 * Query 4 — “Album-level distribution and enrichment coverage”
 *
 * Purpose: for each album, summarizes track-row volume plus enrichment coverage (ISRC presence, composer/lyricist hit rates).
 * Use to confirm certain albums look “complete” and identify albums with missing enrichment.
 */
SELECT t.album_title,
       COUNT(*) AS track_rows,
       COUNT(DISTINCT tr.recording_id) AS distinct_recordings,
       COUNT(DISTINCT tr.isrcs_text) FILTER (WHERE tr.isrcs_text IS NOT NULL) AS recordings_with_isrcs,
       COUNT(*) FILTER (WHERE tr.composer_names_text IS NOT NULL) AS rows_with_composer,
       COUNT(*) FILTER (WHERE tr.lyricist_names_text IS NOT NULL) AS rows_with_lyricist
FROM musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025 tr
JOIN musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025 t
  ON t.tmdb_id = tr.tmdb_id
 AND t.release_group_id = tr.release_group_id
 AND t.match_method = tr.match_method
GROUP BY t.album_title
ORDER BY track_rows DESC, t.album_title
LIMIT 100;

-- Identify album titles with the largest number of tracks
SELECT rg.name AS album_title,
       tr.release_group_id,
       COUNT(*) AS track_ct,
       COUNT(DISTINCT tr.medium_id) AS medium_ct
FROM musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025 tr
JOIN musicbrainz.release_group rg ON rg.id = tr.release_group_id
WHERE lower(rg.name) ~ '(box|complete|collection|anthology|trilogy|saga|vol\\.?|volume|disc|cd|edition)'
GROUP BY rg.name, tr.release_group_id
ORDER BY track_ct DESC
LIMIT 100;
-- There are four albums whose medium_ct is > 1, which are responsible for an enormous number of tracks
-- We will need to decide what to do about that when doing our analysis


st.markdown("""
### CELL 8 — Wide track MV: film→album→track analysis table
""")

st.markdown("""
Builds the final wide, analysis\\-friendly track table by left\\-joining film/album enrichment \\(album spine\\) and aggregated album\\-artist enrichment \\(artist spine\\) onto the track spine backbone, keeping a strict “no row loss” philosophy\\.
""")

/* =============================================================================
MV: musicbrainz.mv_tmdb_soundtrack_wide_track_2015_2025
--------------------------------------------------------------------------------
PURPOSE:
  Wide “film → album → track” table for analysis and modeling.
  This MV is meant to be the easiest thing to export to Python because it already
  contains:
    - Film attributes (TMDB)
    - Link attributes (match method, soundtrack_type, matched_at/notes)
    - Album/release enrichment (titles, tags, labels, US date signals)
    - Track/recording/work enrichment (ISRCs, tags, composer/lyricist)

DATA SOURCES:
  - mv_tmdb_soundtrack_track_spine_2015_2025 (row-per-track backbone)
  - mv_tmdb_soundtrack_album_spine_2015_2025 (film + album enrichment)
  - mv_tmdb_soundtrack_artist_spine_2015_2025 (artist enrichment, aggregated to album)

GRAIN:
  One row per track spine row:
    (tmdb_id, release_group_id, match_method, medium_id, track_id)

NON-NEGOTIABLE RULE:
  - Do NOT introduce filters that remove already-present track spine rows.
    Album and artist enrichments must be LEFT JOINs.

NOTES:
  - Album-artist attributes are aggregated per release_group_id so multi-artist
    credits do not explode the number of track rows.
============================================================================= */

DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_wide_track_2015_2025;

CREATE MATERIALIZED VIEW musicbrainz.mv_tmdb_soundtrack_wide_track_2015_2025 AS
WITH tr AS (
  -- Track spine is the “backbone”: one row per track within the soundtrack universe.
  SELECT *
  FROM musicbrainz.mv_tmdb_soundtrack_track_spine_2015_2025
),
al AS (
  -- Album spine provides film attributes + album/release enrichment per (tmdb_id, release_group_id, match_method).
  SELECT *
  FROM musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025
),
album_artist_ids AS (
  -- Enumerate album-level credited artists via release_group.artist_credit.
  SELECT DISTINCT
         rg.id AS release_group_id,
         acn.artist AS artist_id,
         acn.position AS artist_pos
  FROM musicbrainz.release_group rg
  JOIN musicbrainz.artist_credit_name acn
    ON acn.artist_credit = rg.artist_credit
),
album_artist_enriched AS (
  -- Aggregate album artists into text for easy downstream use.
  SELECT
    aai.release_group_id,
    string_agg(aai.artist_id::text, ' | ' ORDER BY aai.artist_pos, aai.artist_id) AS album_artist_ids_text,
    string_agg(amv.name::text, ' | ' ORDER BY aai.artist_pos, amv.name::text) AS album_artist_names_text,
    string_agg(amv.artist_mbid::text, ' | ' ORDER BY aai.artist_pos, amv.artist_mbid::text) AS album_artist_mbids_text,
    string_agg(COALESCE(amv.artist_type::text, ''), ' | ' ORDER BY aai.artist_pos, amv.name::text) AS album_artist_types_text,
    max(amv.film_ct_in_spine) AS album_artist_max_film_ct_in_spine,
    max(amv.soundtrack_release_group_ct_in_spine) AS album_artist_max_soundtrack_rg_ct_in_spine
  FROM album_artist_ids aai
  JOIN musicbrainz.mv_tmdb_soundtrack_artist_spine_2015_2025 amv
    ON amv.artist_id = aai.artist_id
  GROUP BY aai.release_group_id
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  /* ----------------------------
   * Film attributes (from album spine / TMDB)
   * ---------------------------- */
  tr.tmdb_id,
  COALESCE(al.film_title, tr.film_title) AS film_title, -- Track spine carries film_title; prefer album spine if present.
  -- Keep these in sync with the album spine’s film_* columns.
  al.film_adult,
  al.film_runtime_min,
  al.film_genres,
  al.film_rating,
  al.film_vote_count,
  al.film_mpaa_rating,
  al.film_original_title,
  al.film_language_name,
  al.film_imdb_id,
  al.film_wikidata_id,
  al.film_countries,
  al.film_year,
  al.film_release_date,
  al.film_popularity,
  al.film_budget,
  al.film_revenue,
  al.film_studios,
  al.film_director,
  al.film_soundtrack_composer_raw,
  al.film_top_cast,
  al.film_keywords,
  al.film_ingested_at,
  /* ----------------------------
   * Link / album identity
   * ---------------------------- */
  tr.release_group_id,
  al.release_group_mbid,
  tr.release_id,
  al.release_mbid,
  al.album_title,
  al.rg_primary_type,
  al.rg_secondary_types,
  -- Link metadata (these should exist in the album spine by design).
  tr.match_method,
  al.soundtrack_type AS album_soundtrack_type,
  al.notes AS album_notes,
  al.matched_at AS album_matched_at,
  /* ----------------------------
   * Album US date enrichment + quality flags
   * ---------------------------- */
  al.album_us_release_date,
  al.album_us_release_year,
  al.album_us_release_month_min_observed,
  al.album_us_release_day_min_observed,
  al.us_date_has_missing_month,
  al.us_date_has_missing_day,
  al.us_any_event_missing_month,
  al.us_any_event_missing_day,
  /* ----------------------------
   * Release-level enrichment (canonical release_id)
   * ---------------------------- */
  al.release_title,
  al.release_status,
  al.release_packaging,
  al.barcode,
  al.release_language,
  al.release_script,
  al.release_comment,
  /* ----------------------------
   * Tags / labels / ratings / meta enrichment
   * ---------------------------- */
  al.rg_tags_text,
  al.release_tags_text,
  al.label_names,
  al.label_mbids,
  al.label_tags_text,
  al.rg_rating,
  al.rg_rating_count,
  al.release_meta_date_added,
  al.release_meta_info_url,
  al.release_meta_amazon_asin,
  al.release_meta_cover_art_presence,
  /* ----------------------------
   * Album artist enrichment (aggregated per release_group)
   * ---------------------------- */
  aae.album_artist_ids_text,
  aae.album_artist_names_text,
  aae.album_artist_mbids_text,
  aae.album_artist_types_text,
  aae.album_artist_max_film_ct_in_spine,
  aae.album_artist_max_soundtrack_rg_ct_in_spine,
  /* ----------------------------
   * Track / recording / work enrichment (from track spine)
   * ---------------------------- */
  tr.medium_id,
  tr.disc_number,
  tr.medium_format,
  tr.track_id,
  tr.track_number,
  tr.track_title,
  tr.track_length_ms,
  tr.recording_id,
  tr.recording_mbid,
  tr.recording_title,
  tr.recording_length_ms,
  tr.recording_artist_credit,
  tr.isrcs_text,
  tr.recording_first_release_year,
  tr.recording_first_release_month,
  tr.recording_first_release_day,
  tr.recording_tags_text,
  tr.work_ids_text,
  tr.work_titles_text,
  tr.composer_names_text,
  tr.lyricist_names_text
FROM tr
LEFT JOIN al
  -- LEFT JOIN to avoid dropping track rows if album enrichment is missing for a row.
  ON al.tmdb_id = tr.tmdb_id
 AND al.release_group_id = tr.release_group_id
 AND al.match_method = tr.match_method
LEFT JOIN album_artist_enriched aae
  ON aae.release_group_id = tr.release_group_id;

COMMIT;

select count(*) from musicbrainz.mv_tmdb_soundtrack_wide_track_2015_2025
-- 85842 records with release date fix

st.markdown("""
### CELL 9 — Wide MV validation checks
""")

st.markdown("""
MV\\-internal QA queries ensuring key integrity and basic mapping sanity \\(e\\.g\\., tmdb\\_id has ≥1 linked release group; no unexpected orphan groups\\)\\.
""")

/* -----------------------------------------------------------------------------
1) Check A: release_group_mbid with NO tmdb_id (orphan albums)
If your spine is built correctly, this should be zero.
----------------------------------------------------------------------------- */
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  release_group_mbid,
  COUNT(*) AS rows_in_spine,
  COUNT(DISTINCT tmdb_id) AS tmdb_ids_linked
FROM musicbrainz.mv_tmdb_soundtrack_wide_track_2015_2025
GROUP BY release_group_mbid
HAVING COUNT(DISTINCT tmdb_id) = 0
ORDER BY rows_in_spine DESC, release_group_mbid;

-- empty... Yay!

/* =============================================================================
TEST B (MV-internal):
Ensure EVERY tmdb_id that appears in the MV maps to >= 1 release_group_id
(i.e., no “orphan” tmdb_id rows with NULL release_group_id in this wide MV)

Because the MV is track-grained, we collapse to distinct tmdb_id and then
count distinct release_group_id per tmdb_id.
============================================================================= */

WITH tmdb_album_counts AS (
  SELECT
    tmdb_id,
    COUNT(DISTINCT release_group_id) AS release_group_ct
  FROM musicbrainz.mv_tmdb_soundtrack_wide_track_2015_2025
  WHERE tmdb_id IS NOT NULL
  GROUP BY tmdb_id
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  tmdb_id,
  release_group_ct
FROM tmdb_album_counts
WHERE release_group_ct = 0
ORDER BY tmdb_id;

-- empty -- yay!

st.markdown("""
### CELL 10 — Mapping distribution recap \\(film→soundtrack counts\\)
""")

st.markdown("""
Reproduces the film\\-to\\-soundtrack match distribution reporting to summarize how many soundtrack links each film has \\(and how many are matched vs candidate\\)\\.
""")

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

/* matched_soundtrack_rows   film_ct
 * 6	                        1
 * 5	                        3
 * 4	                        9
 * 3	                        48
 * 2	                        293
 * 1	                        4422
/*

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
### CELL 11 — Album↔artist bridge MV \\(normalized many\\-to\\-many\\)
""")

st.markdown("""
Creates a bridge table connecting each soundtrack album \\(in the album spine\\) to its credited artists with ordering, join phrases, and a deterministic “primary artist” flag — kept separate to avoid row explosion in the album MV\\.
""")

/* =============================================================================
MATERIALIZED VIEW: musicbrainz.mv_tmdb_soundtrack_album_artist_bridge_2015_2025
-------------------------------------------------------------------------------
# auto-detected possible Altair chart: PURPOSE
try:
    st.altair_chart(PURPOSE, use_container_width=True)
except Exception:
    st.write(PURPOSE)
  Bridge table linking each soundtrack album in our TMDB album spine to the
  credited MusicBrainz artists on that album, including:
    - artist credit ordering (position)
    - join phrase (e.g., " & ", " feat. ")
    - a deterministic primary-artist flag (first credited artist)

WHY THIS IS A BRIDGE (NOT IN THE ALBUM MV)
  The album spine’s grain is album/film. Adding artists directly would multiply
  rows (one album → many artists) and can cause double-counting downstream.
  This bridge keeps grains clean and makes joins explicit.

# auto-detected possible Altair chart: GRAIN
try:
    st.altair_chart(GRAIN, use_container_width=True)
except Exception:
    st.write(GRAIN)
  One row per (tmdb_id, release_group_id, artist_credit_id, position)

PRIMARY ARTIST RULE
  We define "primary artist" as the first credited artist on the release_group’s
  artist_credit (minimum position). This matches how MusicBrainz structures
  credits and is stable/reproducible.

JOIN KEYS
  - To Album spine:
      ON (tmdb_id, release_group_id)
  - To Artist spine:
      ON (artist_id)

DATA SOURCES
  - mv_tmdb_soundtrack_album_spine_2015_2025: album universe (tmdb_id × release_group_id)
  - musicbrainz.release_group.artist_credit: credit container per album
  - musicbrainz.artist_credit_name: ordered credited artists per credit
============================================================================= */

DROP MATERIALIZED VIEW IF EXISTS musicbrainz.mv_tmdb_soundtrack_album_artist_bridge_2015_2025;

CREATE MATERIALIZED VIEW musicbrainz.mv_tmdb_soundtrack_album_artist_bridge_2015_2025 AS
WITH spine_rg AS (
  /* Distinct album universe; intentionally drop Album MV extra dimensions. */
  SELECT DISTINCT tmdb_id, release_group_id
  FROM musicbrainz.mv_tmdb_soundtrack_album_spine_2015_2025
),
rg_artist_credit AS (
  /* Attach the artist_credit_id from the release_group. */
  SELECT
    sr.tmdb_id,
    sr.release_group_id,
    rg.artist_credit AS artist_credit_id
  FROM spine_rg sr
  JOIN musicbrainz.release_group rg
    ON rg.id = sr.release_group_id
  WHERE rg.artist_credit IS NOT NULL
),
credit_min_pos AS (
  /* Compute the first credited position per artist_credit (deterministic). */
  SELECT
    acn.artist_credit AS artist_credit_id,
    MIN(acn.position) AS min_position
  FROM musicbrainz.artist_credit_name acn
  GROUP BY acn.artist_credit
)
# auto-detected possible Altair chart: SELECT
try:
    st.altair_chart(SELECT, use_container_width=True)
except Exception:
    st.write(SELECT)
  rac.tmdb_id,
  rac.release_group_id,
  rac.artist_credit_id,
  /* Artist info at the credit-row level */
  acn.position,
  acn.artist AS artist_id,
  /* Useful for reconstructing the exact credit string */
  acn.name::text AS credited_name,
  acn.join_phrase::text AS join_phrase,
  /* Deterministic "primary artist" flag: first credit entry */
  (acn.position = cmp.min_position) AS is_primary_artist
FROM rg_artist_credit rac
JOIN musicbrainz.artist_credit_name acn
  ON acn.artist_credit = rac.artist_credit_id
JOIN credit_min_pos cmp
  ON cmp.artist_credit_id = rac.artist_credit_id;

st.markdown("""
### Final note\\.\\.\\.
""")

st.markdown("""
After executing the DDLs, we export the following materialized views into \\.csv format to do further exploration, cleaning, and analysis in Pytho\\-\\-
""")

st.markdown("""
- MATERIALIZED VIEW musicbrainz\\.mv\\_tmdb\\_soundtrack\\_artist\\_spine\\_2015\\_2025 \\-\\-\\> 2\\.2\\.MUSICBRAINZ\\_mv\\_tmdb\\_soundtrack\\_artist\\_spine\\_2015\\_2025\\_202601241225\\.csv
""")

st.markdown("""
- MATERIALIZED VIEW musicbrainz\\.mv\\_tmdb\\_soundtrack\\_track\\_spine\\_2015\\_2025 \\-\\-\\> 2\\.2\\.MUSICBRAINZ\\_mv\\_tmdb\\_soundtrack\\_track\\_spine\\_2015\\_2025\\_202601241225\\.csv
""")

st.markdown("""
- MATERIALIZED VIEW musicbrainz\\.mv\\_tmdb\\_soundtrack\\_wide\\_track\\_2015\\_2025 \\-\\-\\> 2\\.2\\.MUSICBRAINZ\\_mv\\_tmdb\\_soundtrack\\_wide\\_track\\_2015\\_2025\\_202601241225\\.csv
""")

st.markdown("""
- MATERIALIZED VIEW musicbrainz\\.mv\\_tmdb\\_soundtrack\\_album\\_artist\\_bridge\\_2015\\_2025 \\-\\-\\> 2\\.2\\.MUSICBRAINZ\\_musicbrainz\\.mv\\_tmdb\\_soundtrack\\_album\\_artist\\_bridge\\_2015\\_2025\\.csv
""")
