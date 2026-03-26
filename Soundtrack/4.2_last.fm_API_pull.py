import streamlit as st
import os, sys

st.set_page_config(page_title="4.2 last.fm API pull", layout="wide")

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
# API setup and connection
""")

# Standard library imports
import glob
import os
import time
from datetime import datetime

# Third-party imports
import numpy as np
import pandas as pd
import requests
from tqdm.auto import tqdm

# os.chdir("/work")  # path adjusted for Streamlit
print(os.listdir("./pipeline"))

print("LASTFM_API_KEY present:", "LASTFM_API_KEY" in os.environ)

st.markdown("""
Let's test the API connection
""")

import os, requests

LASTFM_API_KEY = os.environ["LASTFM_API_KEY"]
BASE = "https://ws.audioscrobbler.com/2.0/"

def lastfm(method, **params):
    params.update({"method": method, "api_key": LASTFM_API_KEY, "format": "json"})
    r = requests.get(BASE, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(f"Last.fm error {data['error']}: {data.get('message')}")
    return data

artist = lastfm("artist.getInfo", artist="Taylor Swift")["artist"]

{
    "name": artist.get("name"),
    "mbid": artist.get("mbid"),
    "listeners": int(artist["stats"]["listeners"]),
    "playcount": int(artist["stats"]["playcount"]),
    "url": artist.get("url"),
}

st.markdown("""
Everything seems to be working just fine with the API\\! That's great news\\.
""")

st.markdown("""
Let's get all our dataframes into memory
""")

# Load the key dataframe

albums_df = pd.read_csv("./pipeline/3.7.Albums_composer_analysis.csv")
artists_df = pd.read_csv("./pipeline/3.7.Artists_composer_analysis.csv")
tracks_df = pd.read_csv("./pipeline/3.7.Tracks_composer_analysis.csv")
wide_df = pd.read_csv("./pipeline/3.7.Wide_composer_analysis.csv")

pd.set_option('display.max_columns', None)
pd.set_option("display.width", 200)
print(wide_df.columns.tolist())

st.markdown("""
# II\\. Retrieve track\\-level metrics from last\\.fm
""")

st.markdown("""
### II\\.1 Helper variables and functions
""")

st.markdown("""
Last\\.fm's API is much more relaxed than, say, the Spotify API\\. Nevertheless, to maximize the greatest probability of success, we need to put artificial sleep/wait seconds on each API call, to lessen the number of errors during pull time\\.
""")

# Polite pacing (Last.fm is better than Spotify, but let's not abuse them!)
SLEEP_SECONDS = 0.4
TIMEOUT_SECONDS = 20
LASTFM_BASE_URL = "https://ws.audioscrobbler.com/2.0/"


st.markdown("""
A last\\.fm match using MBID is the most reliable way to pull matching data\\. However, in our tests, we found that only 20% of tracks could be matched using the MBID option\\. Specifically, we use the recording\\_mbid attribute, which is present in the track rows\\. However, leveraging track name and artist name is also possible, and should be used as a secondary matching tactic when mbid matching fails\\. Based on our test data, match rates skyrocket to 92% using both mechanisms\\.
""")

st.markdown("""
The helper function below can be used to retrieve data from the last\\.fm endpoint across both scenarios
""")

# Last.fm helper (track.getInfo) with JSONDecodeError handling

def lastfm_track_getinfo(mbid: str = None, artist_name: str = None, track_title: str = None, autocorrect: int = 1) -> dict:
    """
    Calls Last.fm track.getInfo and returns a JSON dict.
    If the response isn't valid JSON (blank/HTML/etc.), returns an error-shaped payload.
    """
    params = {
        "method": "track.getInfo",
        "api_key": LASTFM_API_KEY,
        "format": "json",
    }

    if mbid:
        params["mbid"] = mbid
    else:
        params["artist"] = artist_name
        params["track"] = track_title
        params["autocorrect"] = autocorrect

    r = requests.get(LASTFM_BASE_URL, params=params, timeout=TIMEOUT_SECONDS)

    # If Last.fm returns a hard HTTP error (rare), treat it as an error payload
    if not r.ok:
        return {
            "error": f"http_{r.status_code}",
            "message": (r.text or "")[:200]
        }

    try:
        return r.json()
    except ValueError:
        # JSONDecodeError is a subclass of ValueError in requests' json() path
        return {
            "error": "json_decode_error",
            "message": (r.text or "")[:200]
        }

st.markdown("""
Artist strings within MusicBrainz can be a bit messy \\(e\\.g\\. "featuring L'il Naz"\\)\\. This helper function makes the best effort attempt at isolating the credited artist's name
""")

def primary_artist(credit: str) -> str:
    """
    Best-effort: take the first credited artist name for Last.fm matching.
    Keeps this simple and deterministic.
    """
    return (credit.split(" feat ")[0]
                 .split(" featuring ")[0]
                 .split(" ft. ")[0]
                 .split(" ft ")[0]
                 .split(" & ")[0]
                 .split(" + ")[0]
                 .split(" / ")[0])

st.markdown("""
This function processes the payload from the last\\.fm API call, appends some necessary metadata, and converts everything to a dictionary for easier processing\\.
""")

# Parse response into a flat row (track-level metrics)

def parse_lastfm_track_info(payload: dict) -> dict:
    """
    Extracts the track-level fields we care about.

    Notes:
    - listeners/playcount come back as strings; cast to int for analysis.
    - Keep resolved (Last.fm) names + URL for auditability.
    - Also capture track_mbid that Last.fm returns (useful for QA).
    """
    if "error" in payload:
        return {
            "lfm_status": "error",
            "lfm_error_code": payload.get("error"),
            "lfm_error_message": payload.get("message"),
            "lfm_track_listeners": None,
            "lfm_track_playcount": None,
            "lfm_track_url": None,
            "lfm_track_name": None,
            "lfm_artist_name": None,
            "lfm_track_mbid": None,
        }

    track = payload.get("track", {})

    return {
        "lfm_status": "ok",
        "lfm_error_code": None,
        "lfm_error_message": None,
        "lfm_track_listeners": int(track.get("listeners")) if track.get("listeners") is not None else None,
        "lfm_track_playcount": int(track.get("playcount")) if track.get("playcount") is not None else None,
        "lfm_track_url": track.get("url"),
        "lfm_track_name": track.get("name"),
        "lfm_artist_name": (track.get("artist") or {}).get("name"),
        "lfm_track_mbid": track.get("mbid"),
    }

st.markdown("""
We will attempt to retrieve tracks from last\\.fm using the recording\\_mbid\\. But how widespread is recording\\_mbid in our tracks data? As you can see below, all our cleaning up has resulted in 100% coverage \\-\\- meaning all track rows in wide\\_df has a populated entry in the recording\\_mbid field\\!
""")

# recording_mbid completeness in wide_df

total_rows = len(wide_df)

rows_with_mbid = wide_df["recording_mbid"].notna().sum()
pct_rows_with_mbid = rows_with_mbid / total_rows

distinct_tracks = wide_df["track_id"].nunique()
distinct_tracks_with_mbid = wide_df.loc[wide_df["recording_mbid"].notna(), "track_id"].nunique()
pct_tracks_with_mbid = distinct_tracks_with_mbid / distinct_tracks

print(f"Rows: {total_rows:,}")
print(f"Rows w/ recording_mbid: {rows_with_mbid:,} ({pct_rows_with_mbid:.2%})")

print(f"\nDistinct track_id: {distinct_tracks:,}")
print(f"Distinct track_id w/ recording_mbid: {distinct_tracks_with_mbid:,} ({pct_tracks_with_mbid:.2%})")

st.markdown("""
### II\\.2 Pulling tracks
""")

st.markdown("""
This is the main API pull function\\. There are ~78K tracks in our dataset, so running this code block will take some time\\. Each 100 retrievals from the API takes about a minute, meaning retrieving 78K rows could take around ~13 hours\\. 
""")

st.markdown("""
Because the last\\.fm API is rather finicky and fails quite a lot \\(every 1\\-2K records\\), we tuned the code to be idempotent and start running at the point where the last run left off\\. We also flush the buffer into the file in \\./api\\_dump/ whenever the function fails\\. This certainly sped up the process\\.
""")

# Pull loop w/ resume + CHUNK checkpointing (wide_df + MBID first, then title/artist fallback)

CHECKPOINT_EVERY = 2000

PARTS_DIR = "./api_dumps/4.1.lastfm_track_parts"                    # NEW: checkpoint parts live here

os.makedirs(PARTS_DIR, exist_ok=True)

# Deterministic processing order for clean resumes (one row per track_id)
tracks_run = (
    wide_df[["track_id", "recording_mbid", "recording_title", "recording_artist_credit"]]
    .drop_duplicates(subset=["track_id"])
    .sort_values("track_id")
)

# Resume: Skip track_ids already saved in any chunk files
done_ids = set()

# glob is a standard Python library (no install) that finds files on disk
# using wildcard patterns—basically “list files that match this naming pattern.”
part_files = sorted(glob.glob(f"{PARTS_DIR}/part_*.csv"))
for p in part_files:
    try:
        done_ids.update(pd.read_csv(p, usecols=["track_id"])["track_id"].tolist())
    except Exception as e:
        print(f"Warning: could not read {p} for resume. Skipping it. Error: {e}")

tracks_todo = tracks_run.loc[~tracks_run["track_id"].isin(done_ids)].copy()

# Helper: next part number (so reruns don’t overwrite existing parts)
if part_files:
    last_part = os.path.basename(part_files[-1]).replace("part_", "").replace(".csv", "")
    next_part_num = int(last_part) + 1
else:
    next_part_num = 1

rows_buffer = []
processed = 0

def write_part(rows, part_num):
    out_path = f"{PARTS_DIR}/part_{part_num:06d}.csv"
    tmp_path = out_path + ".tmp"
    pd.DataFrame(rows).to_csv(tmp_path, index=False)
    os.replace(tmp_path, out_path)
    return out_path

try:
    for _, r in tqdm(tracks_todo.iterrows(), total=len(tracks_todo), desc="Last.fm track.getInfo"):
        # 1) MBID-first
        lfm_query_method = "mbid"
        payload = lastfm_track_getinfo(mbid=r["recording_mbid"])
        parsed = parse_lastfm_track_info(payload)

        # 2) Fallback to text lookup if MBID fails
        if parsed["lfm_status"] == "error":
            lfm_query_method = "text"
            payload = lastfm_track_getinfo(
                mbid=None,
                artist_name=primary_artist(r["recording_artist_credit"]),
                track_title=r["recording_title"],
                autocorrect=1
            )
            parsed = parse_lastfm_track_info(payload)

        pulled_at = pd.Timestamp.utcnow()

        rows_buffer.append({
            "track_id": r["track_id"],
            "recording_mbid": r["recording_mbid"],
            "recording_title": r["recording_title"],
            "recording_artist_credit": r["recording_artist_credit"],
            "lfm_query_method": lfm_query_method,
            "pulled_at": pulled_at,
            **parsed,
        })

        processed += 1
        time.sleep(SLEEP_SECONDS)

        # Checkpoint: write a NEW part file every N rows (no append)
        if processed % CHECKPOINT_EVERY == 0:
            write_part(rows_buffer, next_part_num)
            next_part_num += 1
            rows_buffer = []

finally:
    # Always flush what’s in memory (as a final part file)
    if rows_buffer:
        write_part(rows_buffer, next_part_num)

print("Done (or stopped).")
print("New checkpoint parts dir:", PARTS_DIR)
print("Tracks remaining this run:", len(tracks_todo))
print("Existing part files:", len(part_files))

st.markdown("""
### II\\.3 QAing the append table of tracks
""")

st.markdown("""
Let's QA the final append table before attempting a merge
""")

# Load append + basic shape

PARTS_DIR = "./api_dumps/4.1.lastfm_track_parts"
part_files = sorted(glob.glob(f"{PARTS_DIR}/part_*.csv"))

lfm_append = pd.concat(
    (pd.read_csv(p) for p in part_files),
    ignore_index=True
)

print("Part files:", len(part_files))
print("Rows in append:", len(lfm_append))
print("Distinct track_id:", lfm_append["track_id"].nunique())
print("Distinct pulled_at:", lfm_append["pulled_at"].nunique())

st.markdown("""
Findings: The Last\\.fm append consolidates 31 part files into a single track\\-level table with 78,992 rows—exactly one row per distinct track\\_id, confirming no duplication during chunked API pulls\\. The large number of distinct pulled\\_at timestamps \\(33,338\\) reflects the batched, staggered retrieval we designed rather than repeated track entries\\.
""")

# Status/error distribution

print(lfm_append["lfm_status"].value_counts(dropna=False))

# Top error messages (helps you diagnose rate limits / transient failures)
err = lfm_append.loc[lfm_append["lfm_status"] == "error", "lfm_error_message"]
print("\nTop error messages:")
print(err.value_counts().head(15))

st.markdown("""
Findings: Last\\.fm responses were overwhelmingly successful: 72,427 tracks returned ok status, while 6,565 resulted in errors\\. Nearly all errors \\(6,548\\) were “Track not found,” suggesting coverage gaps rather than systemic API failure\\. True server\\-side issues \\(502 errors or backend failures\\) were rare and likely transient\\.
""")

# Metric sanity (non-null, zeros, weird negatives)

listeners = lfm_append["lfm_track_listeners"]
playcount = lfm_append["lfm_track_playcount"]

print("Non-null listeners:", listeners.notna().sum(), listeners.notna().sum()/len(listeners))
print("Non-null playcount:", playcount.notna().sum(), playcount.notna().sum()/len(playcount))

print("\nZeros:")
print("listeners == 0:", (listeners == 0).sum())
print("playcount == 0:", (playcount == 0).sum())

print("\nNegatives (should be 0):")
print("listeners < 0:", (listeners < 0).sum())
print("playcount < 0:", (playcount < 0).sum())

st.markdown("""
Findings: Metric coverage aligns exactly with API success rates: 72,427 tracks \\(91\\.7%\\) have non\\-null listener and playcount values, matching the “ok” responses\\. There are no zero or negative values, indicating clean, strictly positive engagement metrics and no obvious data corruption in the numeric fields\\.
""")

st.markdown("""
### II\\.4 Append to the wide and track tables
""")

# Read the append table from the .csv into a DataFrame
lastfm_snapshot = lfm_append.copy()

cols_to_add = [
    "track_id",
    "lfm_status",
    "lfm_track_listeners",
    "lfm_track_playcount",
    "lfm_track_url",
    "lfm_query_method",
    "pulled_at",
]

# We don't need all the columns from the append file
lastfm_append_df = lastfm_snapshot[cols_to_add].copy()

# And since we will be pulling lastfm data for albums and artists as well, let's add specificity
# to the column name
lastfm_append_df = lastfm_append_df.rename(columns={
    "lfm_status": "lfm_track_status",
    "lfm_query_method": "lfm_track_query_method",
    "pulled_at": "lfm_track_pulled_at",
})

# Merge to the wide table
before = wide_df.shape

wide_df_mrg = wide_df.merge(
    lastfm_append_df,
    on = 'track_id',
    how = 'left',
    validate = '1:1'
)

print(f"Wide_df before merge shape: {before}")
print(f"Wide_df after  merge shape: {wide_df_mrg.shape}")

# Merge to the track table
before = tracks_df.shape

tracks_df_mrg = tracks_df.merge(
    lastfm_append_df,
    on = 'track_id',
    how = 'left',
    validate = '1:1'
)

print(f"Tracks_df before merge shape: {before}")
print(f"Tracks_df after  merge shape: {tracks_df_mrg.shape}")

st.markdown("""
# III\\. Retrieve album\\-level metrics from last\\.fm
""")

st.markdown("""
Here we retrieve album\\-level last\\.fm listener and playcount metrics\\. The approach is effectively similar to the track\\-level pull
""")

st.markdown("""
### III\\.1 Setup and helper functions
""")

SLEEP_SECONDS = 0.35
TIMEOUT_SECONDS = 20
CHECKPOINT_EVERY = 500
OUT_ALBUMS_CSV = "./api_dumps/4.2.lastfm_album_append.csv"

st.dataframe(albums_df.columns)

def lastfm_get(method: str, params_extra: dict) -> dict:
    """
    Calls Last.fm and returns JSON.
    If the response isn't JSON (blank/HTML/etc.), returns an error-shaped payload
    so your parse_* functions can handle it.
    """
    params = {
        "method": method,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        **params_extra,
    }

    r = requests.get(LASTFM_BASE_URL, params=params, timeout=TIMEOUT_SECONDS)

    if not r.ok:
        return {"error": f"http_{r.status_code}", "message": (r.text or "")[:200]}

    try:
        return r.json()
    except ValueError:
        return {"error": "json_decode_error", "message": (r.text or "")[:200]}

# Similar Helper functions (album.getInfo + parsing)

def lastfm_album_getinfo(mbid: str = None, artist_name: str = None, album_title: str = None, autocorrect: int = 1) -> dict:
    params_extra = {"mbid": mbid} if mbid else {"artist": artist_name, "album": album_title, "autocorrect": autocorrect}
    return lastfm_get("album.getInfo", params_extra)

def parse_lastfm_album_info(payload: dict) -> dict:
    """
    Extract album-level listeners/playcount + URL, with status + error fields.
    """
    if "error" in payload:
        return {
            "lfm_album_status": "error",
            "lfm_album_error_code": payload.get("error"),
            "lfm_album_error_message": payload.get("message"),
            "lfm_album_listeners": None,
            "lfm_album_playcount": None,
            "lfm_album_url": None,
            "lfm_album_name": None,
            "lfm_album_artist_name": None,
            "lfm_album_mbid": None,
        }

    album = payload.get("album", {})

    return {
        "lfm_album_status": "ok",
        "lfm_album_error_code": None,
        "lfm_album_error_message": None,
        "lfm_album_listeners": int(album.get("listeners")) if album.get("listeners") is not None else None,
        "lfm_album_playcount": int(album.get("playcount")) if album.get("playcount") is not None else None,
        "lfm_album_url": album.get("url"),
        "lfm_album_name": album.get("name"),
        "lfm_album_artist_name": (album.get("artist") if isinstance(album.get("artist"), str) else (album.get("artist") or {}).get("name")),
        "lfm_album_mbid": album.get("mbid"),
    }

def primary_album_artist(artists_text: str) -> str:
    if artists_text is None or pd.isna(artists_text) or artists_text.strip() == "":
        return None
    return artists_text.split(" | ")[0]

st.markdown("""
### III\\.2 Main pull loop
""")

st.markdown("""
Build the list of albums from wide\\_df, which has the advantage of carrying all the artist\\-level info as well\\.
""")

# Build the album universe to pull (from wide_df)

# Grain: one row per release_group_mbid (album container in your spine)
albums_run = (
    wide_df[["release_group_mbid", "release_mbid", "album_title", "album_artist_names_text"]]
    .drop_duplicates(subset=["release_group_mbid"])
    .sort_values("release_group_mbid")
)


# Resume: skip albums already saved in OUT_ALBUMS_CSV
if pd.io.common.file_exists(OUT_ALBUMS_CSV):
    done_ids = set(pd.read_csv(OUT_ALBUMS_CSV, usecols=["release_group_mbid"])["release_group_mbid"].tolist())
else:
    done_ids = set()

albums_todo = albums_run.loc[~albums_run["release_group_mbid"].isin(done_ids)].copy()

albums_todo.head()

st.markdown("""
Last\\.fm's coverage for mbid is supposedly weaker for albums than tracks, so it's better to lead with a text\\-based match, and use mbid match as a fallback\\.
""")

def checkpoint_write_album_snapshot(out_csv, new_rows):
    """
    S3FS-safe checkpoint:
      - read existing snapshot (if present)
      - append new rows
      - de-dupe by release_group_mbid, keep latest pulled_at
      - write to .tmp then os.replace
    """
    if pd.io.common.file_exists(out_csv):
        existing = pd.read_csv(out_csv)
        combined = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
    else:
        combined = pd.DataFrame(new_rows)

    combined = (
        combined.sort_values(["release_group_mbid", "lfm_album_pulled_at"])
                .drop_duplicates(subset=["release_group_mbid"], keep="last")
    )

    tmp = out_csv + ".tmp"
    combined.to_csv(tmp, index=False)
    os.replace(tmp, out_csv)

st.markdown("""
This pull loop retrieves Last\\.fm album\\-level metadata with a resilient, restart\\-safe strategy\\. We attempt a text\\-based lookup first \\(artist \\+ album title\\), fall back to MBID when necessary, and buffer results with periodic checkpoint writes to disk\\. This ensures progress is preserved across long\\-running API calls and guards against data loss from interruptions\\.
""")

# Pull loop (text first, MBID fallback) + checkpointing + guaranteed flush
# Strategy:
#   - Query Last.fm using (artist_name, album_title) first (more flexible matching).
#   - If that fails, retry using MusicBrainz release MBID (more precise identifier).
#   - Buffer rows in memory and write to disk in batches (checkpointing).
#   - Always flush remaining rows in a finally block to prevent data loss.

rows_buffer = []
processed = 0

print("Albums total:", len(albums_run))
print("Already done:", len(done_ids))
print("To do:", len(albums_todo))

try:
    for _, r in tqdm(albums_todo.iterrows(), total=len(albums_todo), desc="Last.fm album.getInfo"):
        pulled_at = pd.Timestamp.utcnow()  # timestamp for auditability

        # 1) Text lookup first (artist + album title)
        lfm_album_query_method = "text"
        artist = primary_album_artist(r["album_artist_names_text"])

        # If we can't extract an artist, record a structured error payload
        payload = lastfm_album_getinfo(
            artist_name=artist,
            album_title=r["album_title"],
            autocorrect=1
        ) if artist else {"error": "missing_artist_name", "message": "album_artist_names_text is null/empty"}

        parsed = parse_lastfm_album_info(payload)

        # 2) Fallback to MBID if text lookup fails and we have a release MBID
        if parsed["lfm_album_status"] == "error" and pd.notna(r["release_mbid"]):
            lfm_album_query_method = "mbid"
            payload = lastfm_album_getinfo(mbid=r["release_mbid"])
            parsed = parse_lastfm_album_info(payload)

        # Append structured result (including query method + timestamp)
        rows_buffer.append({
            "release_group_mbid": r["release_group_mbid"],
            "album_title": r["album_title"],
            "album_artist_names_text": r["album_artist_names_text"],
            "lfm_album_query_method": lfm_album_query_method,
            "lfm_album_pulled_at": pulled_at,
            **parsed,
        })

        processed += 1
        time.sleep(SLEEP_SECONDS)  # polite rate limiting

        # Periodically write buffered rows to disk (checkpointing for long runs)
        if processed % CHECKPOINT_EVERY == 0:
            checkpoint_write_album_snapshot(OUT_ALBUMS_CSV, rows_buffer)
            rows_buffer = []

finally:
    # Guaranteed flush of any remaining rows (even if interrupted)
    if rows_buffer:
        checkpoint_write_album_snapshot(OUT_ALBUMS_CSV, rows_buffer)

print("Done (or stopped). Output:", OUT_ALBUMS_CSV)

st.markdown("""
### III\\.3 QA the append table
""")

st.markdown("""
This QA cell validates the integrity and coverage of our Last\\.fm album\\-level append\\. It checks that we have one row per release\\_group\\_mbid, reviews success vs\\. error rates, surfaces the dominant failure mode, and confirms that listener/playcount metrics are populated whenever the API call succeeds\\.
""")

# Cell — QA album append

alb = pd.read_csv(OUT_ALBUMS_CSV)

print("Rows in append:", len(alb))
print("Distinct release_group_mbid:", alb["release_group_mbid"].nunique())
print("Distinct pulled_at:", alb["lfm_album_pulled_at"].nunique())

print("\nStatus distribution:")
print(alb["lfm_album_status"].value_counts(dropna=False))

errs = alb.loc[alb["lfm_album_status"] == "error", "lfm_album_error_message"]
print("\nTop error messages:")
print(errs.value_counts().head(20))

print("\nNon-null listeners:", alb["lfm_album_listeners"].notna().sum())
print("Non-null playcount:", alb["lfm_album_playcount"].notna().sum())

st.markdown("""
Findings: The album append is structurally clean: 4,760 rows and 4,760 distinct release\\_group\\_mbid values, with one unique pull timestamp per row\\. Coverage is high, with 4,703 successful pulls and only 57 failures—all “Album not found\\.” Listener and playcount metrics are present for every successful record \\(4,703\\), matching the ok status count exactly\\.
""")

st.markdown("""
### III\\.4 Merge the append table into albums\\_df and wide\\_df
""")

# Merge album-level Last.fm into wide_df (once the pull is complete)

before = wide_df_mrg.shape

album_latest = (
    alb
    .sort_values(["release_group_mbid", "lfm_album_pulled_at"])
    .drop_duplicates(subset=["release_group_mbid"], keep="last")
)

cols_to_add = [
    "release_group_mbid",
    "lfm_album_status",
    "lfm_album_listeners",
    "lfm_album_playcount",
    "lfm_album_url",
    "lfm_album_query_method",
    "lfm_album_pulled_at",
]

wide_df_w_album = wide_df_mrg.merge(
    album_latest[cols_to_add],
    on="release_group_mbid",
    how="left",
    validate="m:1"   # many track rows per album
)

print("wide_df before:", before)
print("wide_df after: ", wide_df_w_album.shape)

# Cell — Merge album metrics into albums_df

before = albums_df.shape

albums_df_mrg = albums_df.merge(
    album_latest[cols_to_add],
    on="release_group_mbid",
    how="left",
    validate="1:1"
)

print("albums_df before:", before)
print("albums_df after: ", albums_df_mrg.shape)

st.markdown("""
# IV\\. Retrieve artist\\-level metrics from last\\.fm
""")

st.markdown("""
### IV\\.1 Setup and helper functions
""")

# Config (artist pull)

SLEEP_SECONDS = 0.35
TIMEOUT_SECONDS = 20
LASTFM_BASE_URL = "https://ws.audioscrobbler.com/2.0/"

CHECKPOINT_EVERY = 250
OUT_ARTISTS_CSV = "./api_dumps/4.2.lastfm_artist_append.csv"

pulled_at = pd.Timestamp.utcnow()

# Helpers (artist.getInfo + parsing)

def lastfm_artist_getinfo(mbid: str = None, artist_name: str = None, autocorrect: int = 1) -> dict:
    params_extra = {"mbid": mbid} if mbid else {"artist": artist_name, "autocorrect": autocorrect}
    return lastfm_get("artist.getInfo", params_extra)


def parse_lastfm_artist_info(payload: dict) -> dict:
    """
    Extracts artist-level fields we care about.

    Notes:
    - listeners/playcount are strings; cast to int.
    """
    if "error" in payload:
        return {
            "lfm_artist_status": "error",
            "lfm_artist_error_code": payload.get("error"),
            "lfm_artist_error_message": payload.get("message"),
            "lfm_artist_listeners": None,
            "lfm_artist_playcount": None,
            "lfm_artist_url": None,
            "lfm_artist_name": None,
            "lfm_artist_mbid": None,
        }

    artist = payload.get("artist", {})

    stats = artist.get("stats") or {}

    return {
        "lfm_artist_status": "ok",
        "lfm_artist_error_code": None,
        "lfm_artist_error_message": None,
        "lfm_artist_listeners": int(stats.get("listeners")) if stats.get("listeners") is not None else None,
        "lfm_artist_playcount": int(stats.get("playcount")) if stats.get("playcount") is not None else None,
        "lfm_artist_url": artist.get("url"),
        "lfm_artist_name": artist.get("name"),
        "lfm_artist_mbid": artist.get("mbid"),
    }

st.markdown("""
### IV\\.2 Main pull loop
""")

print(artists_df.columns.tolist())

# Artist universe (from artists_df) + resume

artists_run = (
    artists_df[["artist_id", "artist_mbid", "name"]]
    .drop_duplicates(subset=["artist_id"])
    .sort_values("artist_id")
)

# Resume: skip artist_ids already saved in OUT_ARTISTS_CSV
if pd.io.common.file_exists(OUT_ARTISTS_CSV):
    done_ids = set(pd.read_csv(OUT_ARTISTS_CSV, usecols=["artist_id"])["artist_id"].tolist())
else:
    done_ids = set()

artists_todo = artists_run.loc[~artists_run["artist_id"].isin(done_ids)].copy()

st.markdown("""
This loop pulls Last\\.fm artist\\-level metadata using a resilient, restart\\-safe approach\\. We try a stable identifier first \\(MusicBrainz artist MBID\\), fall back to a name\\-based lookup with autocorrect when needed, and write results in checkpoints so long runs can be resumed without losing progress\\.
""")

# Pull loop (MBID first, name fallback) + checkpointing + guaranteed flush
# Strategy:
#   - Query by MusicBrainz artist MBID first (most reliable identifier).
#   - If that fails, retry by artist name using Last.fm autocorrect (more forgiving).
#   - Buffer rows in memory and append to CSV every CHECKPOINT_EVERY rows.
#   - Always flush remaining rows in a finally block to avoid losing partial work.

rows_buffer = []
processed = 0

try:
    for _, r in tqdm(artists_todo.iterrows(), total=len(artists_todo), desc="Last.fm artist.getInfo"):
        pulled_at = pd.Timestamp.utcnow()  # audit timestamp per request/result

        # 1) MBID-first lookup (preferred: stable identifier)
        lfm_artist_query_method = "mbid"
        payload = lastfm_artist_getinfo(mbid=r["artist_mbid"])
        parsed = parse_lastfm_artist_info(payload)

        # 2) Fallback to name-based lookup if MBID fails (handles missing/unsupported MBIDs)
        if parsed["lfm_artist_status"] == "error":
            lfm_artist_query_method = "name"
            payload = lastfm_artist_getinfo(
                mbid=None,
                artist_name=r["name"],
                autocorrect=1
            )
            parsed = parse_lastfm_artist_info(payload)

        # Record a single normalized row per artist (including query method + timestamp)
        rows_buffer.append({
            "artist_id": r["artist_id"],
            "artist_mbid": r["artist_mbid"],
            "artist_name": r["name"],
            "lfm_artist_query_method": lfm_artist_query_method,
            "lfm_artist_pulled_at": pulled_at,
            **parsed,
        })

        processed += 1
        time.sleep(SLEEP_SECONDS)  # polite rate limiting to reduce throttling

        # Checkpoint: append buffered rows to disk periodically
        if processed % CHECKPOINT_EVERY == 0:
            pd.DataFrame(rows_buffer).to_csv(
                OUT_ARTISTS_CSV,
                mode="a",
                header=not pd.io.common.file_exists(OUT_ARTISTS_CSV),
                index=False
            )
            rows_buffer = []  # clear buffer after successful write

finally:
    # Guaranteed flush: write any remaining rows even if the loop errors or is interrupted
    if rows_buffer:
        pd.DataFrame(rows_buffer).to_csv(
            OUT_ARTISTS_CSV,
            mode="a",
            header=not pd.io.common.file_exists(OUT_ARTISTS_CSV),
            index=False
        )

print("Done (or stopped). Output:", OUT_ARTISTS_CSV)

st.markdown("""
### IV\\.3 QA the artist append table
""")

st.markdown("""
This QA cell validates the Last\\.fm artist\\-level append for structural integrity and metric coverage\\. We confirm expected row counts, check whether we have one record per artist, review success vs\\. error rates, and ensure listener/playcount metrics are populated whenever the API returns an ok response\\.
""")

# QA (artist append)

art = pd.read_csv(OUT_ARTISTS_CSV)

print("Rows in append:", len(art))
print("Distinct artist_id:", art["artist_id"].nunique())
print("Distinct pulled_at:", art["lfm_artist_pulled_at"].nunique())

print("\nStatus distribution:")
print(art["lfm_artist_status"].value_counts(dropna=False))

errs = art.loc[art["lfm_artist_status"] == "error", "lfm_artist_error_message"]
print("\nTop error messages:")
print(errs.value_counts().head(20))

print("\nNon-null listeners:", art["lfm_artist_listeners"].notna().sum())
print("Non-null playcount:", art["lfm_artist_playcount"].notna().sum())

st.markdown("""
Findings: The append contains 2,916 rows covering 2,430 distinct artist\\_ids, indicating that some artists were captured more than once \\(likely due to checkpoint appends or re\\-runs\\)\\. Pull timestamps are not behaving as expected—lfm\\_artist\\_pulled\\_at shows only a single distinct value, suggesting the timestamp may not have been set per request or was written incorrectly\\. Despite that, API coverage is strong: 2,864 ok responses with complete listener/playcount metrics, and 52 errors, all driven by “artist not found\\.”
""")

st.markdown("""
### IV\\.4 Merge append table into artists\\_df and wide\\_df
""")

# Merge surfaces (latest per artist)

artist_latest = (
    art.sort_values(["artist_id", "lfm_artist_pulled_at"])
       .drop_duplicates(subset=["artist_id"], keep="last")
)

cols_to_add = [
    "artist_id",
    "lfm_artist_status",
    "lfm_artist_listeners",
    "lfm_artist_playcount",
    "lfm_artist_url",
    "lfm_artist_query_method",
    "lfm_artist_pulled_at",
]

artist_append_small = artist_latest[cols_to_add].copy()

# Merge into artists_df (should be 1:1)

before = artists_df.shape

artists_df_mrg = artists_df.merge(
    artist_append_small,
    on="artist_id",
    how="left",
    validate="1:1"
)

print("artists_df before:", before)
print("artists_df after: ", artists_df_mrg.shape)

st.markdown("""
This cell attaches artist\\-level Last\\.fm metrics to our track\\-grained wide table without inflating row counts\\. Instead of joining directly on the full album\\_artist\\_ids\\_text list \\(which would create one\\-to\\-many duplication\\), we build an album\\-level bridge: select a single “primary album artist” per release\\_group\\_mbid, join that artist to the Last\\.fm artist append, then merge the resulting album\\-level metrics back onto every track on the album via a safe m:1 join\\.
""")

# ------------------------------------------------------------------------------
# BIG PICTURE: Attach Artist-level Last.fm metrics to the track-grained wide table
#
# wide_df_w_album is track-grained (many rows per album), but Artist metrics are
# artist-grained (one row per artist). If we naïvely joined artists to wide_df
# using album_artist_ids_text, we’d either:
#   - explode rows (one track row → multiple artist rows), or
#   - inflate downstream sums/averages.
#
# So this cell creates a safe “bridge” at the album level:
#   1) Collapse to one row per album (release_group_mbid).
#   2) Choose a single “primary album artist” (the first artist_id in the
#      album_artist_ids_text list).
#   3) Join that primary artist_id to the artist append table (Last.fm metrics).
#   4) Merge the resulting album-level artist metrics back onto the track-grained
#      wide_df_w_album via release_group_mbid (m:1), so every track on the album
#      inherits the same primary-artist metrics without changing row counts.
# ------------------------------------------------------------------------------


# One row per album with primary artist_id (first in the ' | '-delimited list)
album_primary_artist = (
    wide_df[["release_group_mbid", "album_artist_ids_text"]]
    .drop_duplicates(subset=["release_group_mbid"])
    .assign(primary_artist_id=lambda d: d["album_artist_ids_text"].str.split(" \\| ").str[0].astype(int))
    [["release_group_mbid", "primary_artist_id"]]
)

# Join artist metrics onto that primary artist_id
album_primary_artist_w = album_primary_artist.merge(
    artist_append_small.rename(columns={"artist_id": "primary_artist_id"}),
    on="primary_artist_id",
    how="left",
    validate="m:1"
)

# Keep only the columns we want to append to wide_df
cols_to_add = [
    "release_group_mbid",
    "primary_artist_id",
    "lfm_artist_status",
    "lfm_artist_listeners",
    "lfm_artist_playcount",
    "lfm_artist_url",
    "lfm_artist_query_method",
    "lfm_artist_pulled_at",
]

album_primary_artist_w = album_primary_artist_w[cols_to_add].rename(columns={
    "primary_artist_id": "album_primary_artist_id",
    "lfm_artist_status": "lfm_album_primary_artist_status",
    "lfm_artist_listeners": "lfm_album_primary_artist_listeners",
    "lfm_artist_playcount": "lfm_album_primary_artist_playcount",
    "lfm_artist_url": "lfm_album_primary_artist_url",
    "lfm_artist_query_method": "lfm_album_primary_artist_query_method",
    "lfm_artist_pulled_at": "lfm_album_primary_artist_pulled_at",
})

# Merge back into wide_df (many tracks per album)
before = wide_df_w_album.shape

wide_df_mrg = wide_df_w_album.merge(
    album_primary_artist_w,
    on="release_group_mbid",
    how="left",
    validate="m:1"
)

print("wide_df before:", before)
print("wide_df after: ", wide_df_mrg.shape)

st.markdown("""
# V\\. Output the tables to /pipeline
""")

st.markdown("""
All of our dataframes now contain the last\\.fm data\\. Let's write them all back into \\.csv for the next part of our pipeline
""")

# Cell — Write appended DataFrames to ./pipeline/

OUT_DIR = "./pipeline"

paths = {
    "Artists": f"{OUT_DIR}/4.2.Artists_lastfm_appended.csv",
    "Albums":  f"{OUT_DIR}/4.2.Albums_lastfm_appended.csv",
    "Tracks":  f"{OUT_DIR}/4.2.Tracks_lastfm_appended.csv",
    "Wide":    f"{OUT_DIR}/4.2.Wide_lastfm_appended.csv",
}

artists_df_mrg.to_csv(paths["Artists"], index=False)
albums_df_mrg.to_csv(paths["Albums"], index=False)
tracks_df_mrg.to_csv(paths["Tracks"], index=False)
wide_df_mrg.to_csv(paths["Wide"], index=False)

print("Wrote:")
for k, p in paths.items():
    print(f"- {k}: {p}")
