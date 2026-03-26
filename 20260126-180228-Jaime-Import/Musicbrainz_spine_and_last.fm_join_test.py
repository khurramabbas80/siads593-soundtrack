import streamlit as st
import os, sys

st.set_page_config(page_title="Musicbrainz spine and last.fm join test", layout="wide")

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
# Setup
""")

st.markdown("""
We set up a minimal, in\\-memory experiment\\. Mode A means we do not persist anything to disk and we accept that if the kernel restarts we rerun calls\\. We keep the sample small so this stays quick and debuggable\\.
""")

import os
import re
import json
import time
import random
from pathlib import Path

import pandas as pd
import requests

JSON_PATH = Path("/work/mv_wide_track_2025_us_202601161706.json")

LASTFM_ENDPOINT = "https://ws.audioscrobbler.com/2.0/"
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")

if not LASTFM_API_KEY:
    raise RuntimeError("Missing LASTFM_API_KEY. Add it to Deepnote environment variables.")

SAMPLE_N = 100
RANDOM_SEED = 42

REQUESTS_PER_SECOND = 4
SLEEP_SECONDS = 1.0 / REQUESTS_PER_SECOND
TIMEOUT_SECONDS = 20
MAX_RETRIES = 3

st.markdown("""
# Step 2\\. Load the JSON spine
""")

st.markdown("""
We load the MusicBrainz spine from JSON and confirm the shape\\. This is just validating that the file path and top\\-level key are correct before we do any API work\\.
""")

with JSON_PATH.open("r", encoding="utf-8") as f:
    raw = json.load(f)

records = raw.get("mv_wide_track_2025_us", [])
df = pd.DataFrame.from_records(records)

print("Rows:", len(df))
print("Cols:", len(df.columns))
df.head(3)

st.markdown("""
We take a reproducible sample so we can iterate quickly and compare runs\\. We keep it at 100 for a first pass; we can raise it later once the logic looks right\\.
""")

df_work = df.sample(n=min(SAMPLE_N, len(df)), random_state=RANDOM_SEED).reset_index(drop=True)

print("Working rows:", len(df_work))
print("MBID present rate:", df_work["recording_mbid"].notna().mean())
print("Artist present rate:", df_work["album_artist_credit"].notna().mean())
print("Track present rate:", df_work["track_title"].notna().mean())

df_work[["recording_mbid", "album_artist_credit", "track_title", "album_title", "album_us_release_date"]].head(10)

st.markdown("""
# Step 3\\. Helper Functions
""")

st.markdown("""
We prepare the fields we’ll use to match\\. MBID is our first\\-choice lookup key\\. We also prepare normalized artist and track strings for fallback matching because MBID coverage in Last\\.fm is not guaranteed\\. Normalization reduces trivial mismatches like curly apostrophes and spacing\\.
""")

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

df_work["artist_name"] = df_work["album_artist_credit"].fillna("").astype(str)
df_work["track_name"] = df_work["track_title"].fillna("").astype(str)

df_work["artist_norm"] = df_work["artist_name"].map(normalize_text)
df_work["track_norm"] = df_work["track_name"].map(normalize_text)

df_work["recording_mbid"] = df_work["recording_mbid"].fillna("").astype(str).str.strip()
df_work.loc[df_work["recording_mbid"] == "", "recording_mbid"] = None

df_work.head(3)[["recording_mbid", "artist_name", "track_name", "artist_norm", "track_norm"]]

st.markdown("""
We define a small Last\\.fm client and a parser that converts Last\\.fm’s nested response into stable columns\\. We also standardize failures into a single shape so “unmatched” is not confused with “API error\\.”
""")

def lastfm_request(params: dict) -> dict:
    base_params = {"api_key": LASTFM_API_KEY, "format": "json", **params}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(LASTFM_ENDPOINT, params=base_params, timeout=TIMEOUT_SECONDS)
            if resp.status_code == 429:
                time.sleep(2.0 + random.random())
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("error"):
                return {"_lastfm_error": data.get("message", "Unknown Last.fm error"), "_raw": data}
            return data
        except Exception as e:
            if attempt == MAX_RETRIES:
                return {"_lastfm_error": str(e)}
            time.sleep((2 ** (attempt - 1)) + random.random())

def parse_track_getinfo(data: dict) -> dict:
    if not isinstance(data, dict) or data.get("_lastfm_error"):
        return {
            "matched_lastfm": False,
            "lastfm_error": data.get("_lastfm_error", None),
            "lastfm_listeners": None,
            "lastfm_playcount": None,
            "lastfm_track_name": None,
            "lastfm_artist_name": None,
            "lastfm_toptags": None,
        }

    track = data.get("track")
    if not isinstance(track, dict):
        return {
            "matched_lastfm": False,
            "lastfm_error": "No 'track' object in response",
            "lastfm_listeners": None,
            "lastfm_playcount": None,
            "lastfm_track_name": None,
            "lastfm_artist_name": None,
            "lastfm_toptags": None,
        }

    tags_obj = track.get("toptags", {})
    tags = tags_obj.get("tag", []) if isinstance(tags_obj, dict) else []
    if isinstance(tags, dict):
        tags = [tags]
    tag_names = [t.get("name") for t in tags if isinstance(t, dict) and t.get("name")]
    tag_names = tag_names[:20]

    return {
        "matched_lastfm": True,
        "lastfm_error": None,
        "lastfm_listeners": int(track["listeners"]) if str(track.get("listeners", "")).isdigit() else None,
        "lastfm_playcount": int(track["playcount"]) if str(track.get("playcount", "")).isdigit() else None,
        "lastfm_track_name": track.get("name") or None,
        "lastfm_artist_name": (track.get("artist", {}) or {}).get("name") if isinstance(track.get("artist", {}), dict) else None,
        "lastfm_toptags": "|".join(tag_names) if tag_names else None,
    }

st.markdown("""
# MBID only Enrichment
""")

st.markdown("""
We do MBID\\-only enrichment first\\. We de\\-duplicate MBIDs so we call Last\\.fm once per unique track MBID in the sample, then merge results back to all rows\\. This gives us a clean “MBID match rate” baseline\\.
""")

mbids = df_work.loc[df_work["recording_mbid"].notna(), "recording_mbid"].drop_duplicates().tolist()
print("Unique MBIDs to test:", len(mbids))

mbid_results = []
for i, mbid in enumerate(mbids, start=1):
    data = lastfm_request({"method": "track.getInfo", "mbid": mbid})
    parsed = parse_track_getinfo(data)
    parsed["recording_mbid"] = mbid
    mbid_results.append(parsed)

    time.sleep(SLEEP_SECONDS)
    if i % 25 == 0 or i == len(mbids):
        print(f"MBID progress: {i}/{len(mbids)}")

df_mbid = pd.DataFrame(mbid_results)
df_mbid.head(5)

st.markdown("""
We join MBID results back to the sample spine and compute the MBID\\-only match rate\\. Then we create a “needs fallback” subset for rows that didn’t match via MBID\\.
""")

df_joined = df_work.merge(df_mbid, how="left", on="recording_mbid", suffixes=("", "_mbid"))

df_joined["matched_mbid"] = df_joined["matched_lastfm"].fillna(False).astype(bool)

total = len(df_joined)
matched_mbid = int(df_joined["matched_mbid"].sum())
print("Total sample rows:", total)
print("Matched via MBID:", matched_mbid)
print("MBID match rate:", matched_mbid / total)

needs_fallback = df_joined.loc[~df_joined["matched_mbid"]].copy()
print("Rows needing fallback:", len(needs_fallback))
needs_fallback[["artist_name", "track_name", "artist_norm", "track_norm", "recording_mbid"]].head(10)

st.markdown("""
# Fallback Enrichment
""")

st.markdown("""
We run fallback enrichment only for the rows that MBID didn’t match\\. We de\\-duplicate by normalized artist\\+track so we don’t spam the API\\. Then we merge fallback results back and compute the final “augmented %” \\(MBID or fallback\\)\\.
""")

pairs = (
    needs_fallback.loc[(needs_fallback["artist_norm"] != "") & (needs_fallback["track_norm"] != ""), ["artist_name", "track_name", "artist_norm", "track_norm"]]
    .drop_duplicates(subset=["artist_norm", "track_norm"])
    .reset_index(drop=True)
)
print("Unique fallback artist+track lookups:", len(pairs))

fallback_results = []
for i, row in pairs.iterrows():
    artist = row["artist_name"]
    track = row["track_name"]
    data = lastfm_request({"method": "track.getInfo", "artist": artist, "track": track, "autocorrect": 1})
    parsed = parse_track_getinfo(data)
    parsed["artist_norm"] = row["artist_norm"]
    parsed["track_norm"] = row["track_norm"]
    fallback_results.append(parsed)

    time.sleep(SLEEP_SECONDS)
    if (i + 1) % 25 == 0 or (i + 1) == len(pairs):
        print(f"Fallback progress: {i+1}/{len(pairs)}")

df_fallback = pd.DataFrame(fallback_results)

df_final = df_joined.merge(df_fallback, how="left", on=["artist_norm", "track_norm"], suffixes=("", "_fb"))

df_final["matched_fallback"] = df_final["matched_lastfm_fb"].fillna(False).astype(bool)
df_final["matched_any"] = df_final["matched_mbid"] | df_final["matched_fallback"]

matched_any = int(df_final["matched_any"].sum())
matched_fb_only = int((~df_final["matched_mbid"] & df_final["matched_fallback"]).sum())

print("Matched via MBID:", matched_mbid, "| rate:", matched_mbid / total)
print("Matched via fallback only:", matched_fb_only, "| rate:", matched_fb_only / total)
print("Matched via MBID or fallback:", matched_any, "| rate:", matched_any / total)

df_final[["artist_name", "track_name", "matched_mbid", "matched_fallback", "matched_any", "lastfm_listeners", "lastfm_playcount", "lastfm_toptags"]].head(20)

st.markdown("""
## Short findings on Track\\-level matching
""")

st.markdown("""
In a 100\\-track sample of 2025 US MusicBrainz releases, Last\\.fm enrichment succeeded for 83% of tracks when combining MBID\\-based and artist\\+track fallback matching\\. MBID\\-only matching covered 44% of tracks, while an additional 39% were recovered via name\\-based fallback, indicating that MBID coverage in Last\\.fm is incomplete and fallback matching is necessary for meaningful catalog overlap measurement\\. Overall results suggest strong cross\\-platform coverage but reinforce the need to track match method as a quality signal
""")

st.markdown("""
# Artist Augmentation
""")

st.markdown("""
We perform artist\\-level enrichment using Last\\.fm’s artist\\.getInfo endpoint\\. Artist enrichment is more stable and complete than track\\-level enrichment because artist entities are better represented in Last\\.fm’s catalog and are less sensitive to release recency or track\\-level scrobbling behavior\\. We de\\-duplicate artists first so we only call the API once per unique artist, then join the results back to all rows in the working dataframe\\. This gives us artist\\-level popularity and genre signals that can be reused across every track by that artist\\.
""")

st.markdown("""
We treat artist matching as successful if Last\\.fm returns a valid artist object, and we capture listeners, playcount, and top tags\\. This step produces both an artist\\-level enrichment table and artist\\-level match flags that can be analyzed independently or combined with track\\-level coverage\\.
""")

# Collect unique artists from the working sample
artists = (
    df_work.loc[df_work["artist_norm"] != "", ["artist_name", "artist_norm"]]
    .drop_duplicates(subset=["artist_norm"])
    .reset_index(drop=True)
)

print("Unique artists to enrich:", len(artists))
artists.head(10)

# Run artist-level enrichment (Mode A, in-memory)

artist_results = []

for i, row in artists.iterrows():
    artist_name = row["artist_name"]
    artist_norm = row["artist_norm"]

    data = lastfm_request({
        "method": "artist.getInfo",
        "artist": artist_name,
        "autocorrect": 1,
    })

    if not isinstance(data, dict) or data.get("_lastfm_error"):
        parsed = {
            "matched_artist": False,
            "artist_error": data.get("_lastfm_error", None),
            "artist_listeners": None,
            "artist_playcount": None,
            "artist_name_lastfm": None,
            "artist_toptags": None,
        }
    else:
        artist_obj = data.get("artist")
        if not isinstance(artist_obj, dict):
            parsed = {
                "matched_artist": False,
                "artist_error": "No 'artist' object in response",
                "artist_listeners": None,
                "artist_playcount": None,
                "artist_name_lastfm": None,
                "artist_toptags": None,
            }
        else:
            tags_obj = artist_obj.get("tags", {})
            tags = tags_obj.get("tag", []) if isinstance(tags_obj, dict) else []
            if isinstance(tags, dict):
                tags = [tags]
            tag_names = [t.get("name") for t in tags if isinstance(t, dict) and t.get("name")]
            tag_names = tag_names[:20]

            parsed = {
                "matched_artist": True,
                "artist_error": None,
                "artist_listeners": int(artist_obj["stats"]["listeners"]) if str(artist_obj.get("stats", {}).get("listeners", "")).isdigit() else None,
                "artist_playcount": int(artist_obj["stats"]["playcount"]) if str(artist_obj.get("stats", {}).get("playcount", "")).isdigit() else None,
                "artist_name_lastfm": artist_obj.get("name"),
                "artist_toptags": "|".join(tag_names) if tag_names else None,
            }

    parsed["artist_norm"] = artist_norm
    artist_results.append(parsed)

    time.sleep(SLEEP_SECONDS)

    if (i + 1) % 25 == 0 or (i + 1) == len(artists):
        print(f"Artist progress: {i+1}/{len(artists)}")

df_artist = pd.DataFrame(artist_results)
df_artist.head(10)

# Join artist enrichment back to the working dataframe

df_artist_final = df_work.merge(
    df_artist,
    how="left",
    on="artist_norm"
)

total_rows = len(df_artist_final)
matched_artists = int(df_artist_final["matched_artist"].fillna(False).sum())

print("Total rows:", total_rows)
print("Rows with matched artist:", matched_artists)
print("Artist match rate:", matched_artists / total_rows)

df_artist_final[
    ["artist_name", "matched_artist", "artist_listeners", "artist_playcount", "artist_toptags"]
].head(20)

st.markdown("""
## Artist enrichment findings
""")

st.markdown("""
Artist\\-level enrichment using Last\\.fm’s artist\\.getInfo endpoint achieved complete coverage of unique artists in the 100\\-track sample, with 100% of distinct artists successfully matched\\. Artist\\-level metrics such as listeners, playcount, and top tags were consistently populated and far more stable than track\\-level metrics, confirming that artist entities are well\\-represented in Last\\.fm’s catalog\\. This makes artist enrichment a high\\-confidence, low\\-noise augmentation layer that generalizes cleanly across all tracks by the same artist\\.
""")

st.markdown("""
# Album Augmentation
""")

st.markdown("""
We add album\\-level enrichment using Last\\.fm’s album\\.getInfo endpoint\\. Album enrichment is useful because some listening behavior aggregates more cleanly at the album level than at the track level, and album tags can be more stable than track tags\\. We de\\-duplicate by a normalized album key so we only call the API once per unique artist\\+album combination, then join the album attributes back to every track row associated with that album in the working sample\\. We treat an album as matched if Last\\.fm returns a valid album object, and we capture playcount, listeners, and top tags where available\\.
""")

# Build unique artist+album pairs from df_work

albums = (
    df_work.loc[(df_work["artist_norm"] != "") & (df_work["album_title"].notna()), ["artist_name", "artist_norm", "album_title"]]
    .copy()
)

albums["album_name"] = albums["album_title"].fillna("").astype(str)
albums["album_norm"] = albums["album_name"].map(normalize_text)

albums = (
    albums.loc[(albums["album_norm"] != "")]
    .drop_duplicates(subset=["artist_norm", "album_norm"])
    .reset_index(drop=True)
)

print("Unique artist+album pairs to enrich:", len(albums))
albums.head(10)

# Run album-level enrichment (Mode A, in-memory)

album_results = []

for i, row in albums.iterrows():
    artist_name = row["artist_name"]
    artist_norm = row["artist_norm"]
    album_name = row["album_name"]
    album_norm = row["album_norm"]

    data = lastfm_request({
        "method": "album.getInfo",
        "artist": artist_name,
        "album": album_name,
        "autocorrect": 1,
    })

    if not isinstance(data, dict) or data.get("_lastfm_error"):
        parsed = {
            "matched_album": False,
            "album_error": data.get("_lastfm_error", None),
            "album_listeners": None,
            "album_playcount": None,
            "album_name_lastfm": None,
            "album_artist_lastfm": None,
            "album_toptags": None,
        }
    else:
        album_obj = data.get("album")
        if not isinstance(album_obj, dict):
            parsed = {
                "matched_album": False,
                "album_error": "No 'album' object in response",
                "album_listeners": None,
                "album_playcount": None,
                "album_name_lastfm": None,
                "album_artist_lastfm": None,
                "album_toptags": None,
            }
        else:
            tags_obj = album_obj.get("tags", {})
            tags = tags_obj.get("tag", []) if isinstance(tags_obj, dict) else []
            if isinstance(tags, dict):
                tags = [tags]
            tag_names = [t.get("name") for t in tags if isinstance(t, dict) and t.get("name")]
            tag_names = tag_names[:20]

            parsed = {
                "matched_album": True,
                "album_error": None,
                "album_listeners": int(album_obj.get("listeners")) if str(album_obj.get("listeners", "")).isdigit() else None,
                "album_playcount": int(album_obj.get("playcount")) if str(album_obj.get("playcount", "")).isdigit() else None,
                "album_name_lastfm": album_obj.get("name"),
                "album_artist_lastfm": album_obj.get("artist"),
                "album_toptags": "|".join(tag_names) if tag_names else None,
            }

    parsed["artist_norm"] = artist_norm
    parsed["album_norm"] = album_norm
    parsed["album_name"] = album_name
    album_results.append(parsed)

    time.sleep(SLEEP_SECONDS)

    if (i + 1) % 25 == 0 or (i + 1) == len(albums):
        print(f"Album progress: {i+1}/{len(albums)}")

df_album = pd.DataFrame(album_results)
df_album.head(10)

# Fix: compute unique album match stats without relying on a separate album_name column

total_rows = len(df_album_final)
matched_album_rows = int(df_album_final["matched_album"].fillna(False).sum())

unique_albums = df_album_final[["artist_norm", "album_norm", "album_title"]].drop_duplicates()
unique_album_total = len(unique_albums)

unique_album_matches = df_album_final[["artist_norm", "album_norm", "matched_album"]].drop_duplicates()
unique_album_matched = int(unique_album_matches["matched_album"].fillna(False).sum())

print("ALBUM ENRICHMENT SUMMARY")
print("Total rows:", total_rows)
print("Rows with matched album:", matched_album_rows, "| rate:", matched_album_rows / total_rows)
print("Unique albums:", unique_album_total)
print("Unique albums matched:", unique_album_matched, "| rate:", unique_album_matched / unique_album_total)

df_album_final[["artist_name", "album_title", "matched_album", "album_listeners", "album_playcount", "album_toptags"]].head(20)

st.markdown("""
## Album enrichment findings
""")

st.markdown("""
Album\\-level enrichment via Last\\.fm’s album\\.getInfo endpoint produced very high coverage, matching approximately 98% of rows and 97\\.8% of unique artist\\-album pairs in the sample\\. Album\\-level popularity and tagging data were broadly available and tended to smooth out sparsity observed at the track level, especially for newer releases\\. These results indicate that album enrichment is both reliable and complementary to artist\\-level signals, providing a strong release\\-centric layer for downstream analysis\\.
""")

st.markdown("""
# Showcase what we pulled
""")

st.markdown("""
We showcase a small, human\\-readable sample of the augmented dataset by selecting a handful of rows and displaying the key MusicBrainz spine fields alongside the three enrichment layers: track \\(Last\\.fm track\\.getInfo\\), artist \\(artist\\.getInfo\\), and album \\(album\\.getInfo\\)\\. We also include the match flags so it’s obvious which enrichment paths succeeded for each row\\.
""")

# Showcase: sample of augmented data (Track + Artist + Album)

SHOW_N = 20
RANDOM_SEED_SHOWCASE = 7

# Start from track-enriched df_final, then join in artist + album enrichment
df_show = df_final.copy()

# Join artist enrichment (from df_artist) onto df_show
df_show = df_show.merge(
    df_artist[["artist_norm", "matched_artist", "artist_listeners", "artist_playcount", "artist_toptags"]],
    how="left",
    on="artist_norm"
)

# Join album enrichment (from df_album) onto df_show
# Ensure df_show has album_norm the same way we built it earlier
df_show["album_norm"] = df_show["album_title"].fillna("").astype(str).map(normalize_text)

df_show = df_show.merge(
    df_album[["artist_norm", "album_norm", "matched_album", "album_listeners", "album_playcount", "album_toptags"]],
    how="left",
    on=["artist_norm", "album_norm"]
)

# Pick a mixed sample: some matched tracks, some unmatched tracks
matched_sample = df_show[df_show["matched_any"]].sample(
    n=min(SHOW_N // 2, int(df_show["matched_any"].sum())),
    random_state=RANDOM_SEED_SHOWCASE
)

unmatched_sample = df_show[~df_show["matched_any"]].sample(
    n=min(SHOW_N - len(matched_sample), int((~df_show["matched_any"]).sum())),
    random_state=RANDOM_SEED_SHOWCASE
)

sample_rows = pd.concat([matched_sample, unmatched_sample], ignore_index=True)

cols = [
    # Spine
    "album_us_release_date", "artist_name", "album_title", "track_name",
    "recording_mbid",
    # Match flags
    "matched_mbid", "matched_fallback", "matched_any",
    "matched_artist", "matched_album",
    # Track enrichment
    "lastfm_listeners", "lastfm_playcount", "lastfm_toptags",
    # Artist enrichment
    "artist_listeners", "artist_playcount", "artist_toptags",
    # Album enrichment
    "album_listeners", "album_playcount", "album_toptags",
]

# Display (trim long tags for readability)
out = sample_rows[cols].copy()
for c in ["lastfm_toptags", "artist_toptags", "album_toptags"]:
    out[c] = out[c].fillna("").astype(str).map(lambda s: (s[:120] + "…") if len(s) > 120 else s)

out.sort_values(["matched_any", "matched_artist", "matched_album"], ascending=False).reset_index(drop=True).head(SHOW_N)

st.markdown("""
# Export to \\.csv
""")

EXPORT_PATH = "/work/lastfm_enrichment_sample_100_rows.csv"
df_show.to_csv(EXPORT_PATH, index=False)
# auto-detected possible Altair chart: EXPORT_PATH
try:
    st.altair_chart(EXPORT_PATH, use_container_width=True)
except Exception:
    st.write(EXPORT_PATH)
