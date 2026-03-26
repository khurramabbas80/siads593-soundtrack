import streamlit as st
import os, sys

st.set_page_config(page_title="Last.fm Exploration", layout="wide")

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

import os

print("LASTFM_API_KEY present:", "LASTFM_API_KEY" in os.environ)

st.markdown("""
# Basic Retrieval Tests
""")

import os, requests

API_KEY = os.environ["LASTFM_API_KEY"]
BASE = "https://ws.audioscrobbler.com/2.0/"

def lastfm(method, **params):
    params.update({"method": method, "api_key": API_KEY, "format": "json"})
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

import pandas as pd

data = lastfm("chart.getTopArtists", limit=50, page=1)
rows = data["artists"]["artist"]

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return None

def pick_image(images, size="extralarge"):
    # images is a list of dicts like {"#text": "...", "size": "small"}
    if not images:
        return None
    for im in images:
        if im.get("size") == size and im.get("#text"):
            return im["#text"]
    # fallback: first non-empty
    for im in images:
        if im.get("#text"):
            return im["#text"]
    return None

df_top_artists = pd.DataFrame([{
    "rank": i + 1,  # computed rank based on order returned
    "name": a.get("name"),
    "mbid": a.get("mbid") or None,
    "listeners": safe_int(a.get("listeners")),
    "playcount": safe_int(a.get("playcount")),
    "url": a.get("url"),
    "image_url": pick_image(a.get("image", []), size="extralarge"),
} for i, a in enumerate(rows)])

df_top_artists.head(10)

import time
import pandas as pd

def get_top_artists(pages=10, limit=50, sleep=0.25):
    all_rows = []
    for page in range(1, pages + 1):
        data = lastfm("chart.getTopArtists", limit=limit, page=page)
        rows = data["artists"]["artist"]
        for i, a in enumerate(rows):
            all_rows.append({
                "rank_global": (page - 1) * limit + (i + 1),
                "name": a.get("name"),
                "mbid": a.get("mbid") or None,
                "listeners": safe_int(a.get("listeners")),
                "playcount": safe_int(a.get("playcount")),
                "url": a.get("url"),
            })
        time.sleep(sleep)  # be polite
    return pd.DataFrame(all_rows)

df_top500 = get_top_artists(pages=10, limit=50)
df_top500.tail(3), df_top500.shape

def normalize_mbid(x):
    x = (x or "").strip()
    return x if x else None

def lastfm_with_retry(method, max_retries=3, backoff=1.7, **params):
    """
    Last.fm is usually stable, but you can still get intermittent errors or throttling.
    This wrapper retries a few times with exponential-ish backoff.
    """
    attempt = 0
    while True:
        try:
            return lastfm(method, **params)
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise
            # gentle backoff
            sleep_s = (backoff ** (attempt - 1)) * 0.6
            print(f"[retry {attempt}/{max_retries}] {method} failed: {type(e).__name__}: {str(e)[:120]} ... sleeping {sleep_s:.2f}s")
            time.sleep(sleep_s)

# ----------------------------
# 1) Single-artist “shape check” (optional but useful)
# ----------------------------
artist_test = "The Weeknd"

top_albums_json = lastfm_with_retry("artist.getTopAlbums", artist=artist_test, limit=5, page=1)
top_tracks_json = lastfm_with_retry("artist.getTopTracks", artist=artist_test, limit=5, page=1)

albums_rows = top_albums_json["topalbums"]["album"]
tracks_rows = top_tracks_json["toptracks"]["track"]

print("Sample album row:\n", albums_rows[0])
print("\nSample track row:\n", tracks_rows[0])

# ----------------------------
# 2) Extract Top Albums / Top Tracks for a list of artists (Top 500)
# ----------------------------

def extract_top_albums_for_artist(artist_name, artist_mbid=None, limit=200, page=1):
    data = lastfm_with_retry("artist.getTopAlbums", artist=artist_name, limit=limit, page=page)
    rows = data["topalbums"]["album"]

    out = []
    for i, a in enumerate(rows):
        # album artist can be a dict or a string depending on endpoint/artist
        album_artist = a.get("artist")
        album_artist_name = album_artist.get("name") if isinstance(album_artist, dict) else album_artist

        out.append({
            "artist_name": artist_name,
            "artist_mbid": normalize_mbid(artist_mbid),
            "artist_rank_global": None,  # optional: join later from df_top500 if you want
            "album_rank_for_artist": i + 1,
            "album_name": a.get("name"),
            "album_mbid": normalize_mbid(a.get("mbid")),
            "album_url": a.get("url"),
            "album_playcount": safe_int(a.get("playcount")),
            # Some payloads use listeners; others may omit it. Keep it nullable.
            "album_listeners": safe_int(a.get("listeners")),
            "album_artist_name": album_artist_name,
            "image_url": pick_image(a.get("image", []), size="extralarge"),
        })
    return out

def extract_top_tracks_for_artist(artist_name, artist_mbid=None, limit=200, page=1):
    data = lastfm_with_retry("artist.getTopTracks", artist=artist_name, limit=limit, page=page)
    rows = data["toptracks"]["track"]

    out = []
    for i, t in enumerate(rows):
        track_artist = t.get("artist")
        track_artist_name = track_artist.get("name") if isinstance(track_artist, dict) else track_artist

        out.append({
            "artist_name": artist_name,
            "artist_mbid": normalize_mbid(artist_mbid),
            "artist_rank_global": None,  # optional: join later from df_top500 if you want
            "track_rank_for_artist": i + 1,
            "track_name": t.get("name"),
            "track_mbid": normalize_mbid(t.get("mbid")),
            "track_url": t.get("url"),
            "track_playcount": safe_int(t.get("playcount")),
            "track_listeners": safe_int(t.get("listeners")),
            "track_artist_name": track_artist_name,
            "image_url": pick_image(t.get("image", []), size="extralarge"),
            "streamable": (t.get("streamable", {}).get("#text") if isinstance(t.get("streamable"), dict) else t.get("streamable")),
        })
    return out


def pull_top500_artist_albums_tracks(
    df_artists,
    album_limit_per_artist=200,
    track_limit_per_artist=200,
    sleep_s=0.25,
    checkpoint_every=25,
    out_prefix="lastfm_top500",
):
    """
    Pulls:
      - artist.getTopAlbums for each artist
      - artist.getTopTracks for each artist
    and periodically writes checkpoints to CSV.
    """
    # Map artist -> global rank from df_top500 (computed when you built it)
    rank_map = {}
    if "rank_global" in df_artists.columns:
        rank_map = dict(zip(df_artists["name"], df_artists["rank_global"]))
    elif "rank_global" not in df_artists.columns and "rank" in df_artists.columns:
        # if you used "rank" as computed rank earlier
        rank_map = dict(zip(df_artists["name"], df_artists["rank"]))

    albums_all = []
    tracks_all = []
    errors = []

    for idx, row in df_artists.reset_index(drop=True).iterrows():
        artist_name = row["name"]
        artist_mbid = row.get("mbid", None)
        artist_rank = rank_map.get(artist_name)

        try:
            albums = extract_top_albums_for_artist(
                artist_name,
                artist_mbid=artist_mbid,
                limit=album_limit_per_artist,
                page=1,
            )
            for a in albums:
                a["artist_rank_global"] = artist_rank
            albums_all.extend(albums)
        except Exception as e:
            errors.append({"artist": artist_name, "which": "albums", "error": f"{type(e).__name__}: {str(e)}"})

        time.sleep(sleep_s)

        try:
            tracks = extract_top_tracks_for_artist(
                artist_name,
                artist_mbid=artist_mbid,
                limit=track_limit_per_artist,
                page=1,
            )
            for t in tracks:
                t["artist_rank_global"] = artist_rank
            tracks_all.extend(tracks)
        except Exception as e:
            errors.append({"artist": artist_name, "which": "tracks", "error": f"{type(e).__name__}: {str(e)}"})

        time.sleep(sleep_s)

        # Checkpoint
        if (idx + 1) % checkpoint_every == 0:
            df_alb_ckpt = pd.DataFrame(albums_all)
            df_trk_ckpt = pd.DataFrame(tracks_all)
            df_err_ckpt = pd.DataFrame(errors)

            df_alb_ckpt.to_csv(f"{out_prefix}_artist_top_albums_checkpoint.csv", index=False)
            df_trk_ckpt.to_csv(f"{out_prefix}_artist_top_tracks_checkpoint.csv", index=False)
            df_err_ckpt.to_csv(f"{out_prefix}_errors_checkpoint.csv", index=False)

            print(f"[checkpoint] artists processed: {idx+1}/{len(df_artists)} | albums rows: {len(df_alb_ckpt)} | tracks rows: {len(df_trk_ckpt)} | errors: {len(df_err_ckpt)}")

    # Final outputs
    df_albums = pd.DataFrame(albums_all)
    df_tracks = pd.DataFrame(tracks_all)
    df_errors = pd.DataFrame(errors)

    df_albums.to_csv(f"{out_prefix}_artist_top_albums.csv", index=False)
    df_tracks.to_csv(f"{out_prefix}_artist_top_tracks.csv", index=False)
    df_errors.to_csv(f"{out_prefix}_errors.csv", index=False)

    return df_albums, df_tracks, df_errors


# --- Run it (this will make ~1000 calls; keep sleep_s >= 0.2 to be polite)
df_artist_top_albums, df_artist_top_tracks, df_lastfm_errors = pull_top500_artist_albums_tracks(
    df_top500,
    album_limit_per_artist=200,
    track_limit_per_artist=200,
    sleep_s=0.25,
    checkpoint_every=25,
    out_prefix="lastfm_top500",
)

df_artist_top_albums.shape, df_artist_top_tracks.shape, df_lastfm_errors.shape

st.dataframe(df_artist_top_albums.head(5))
st.dataframe(df_artist_top_tracks.head(5))
st.dataframe(df_lastfm_errors.head(5))

print(df_top_artists.columns)
print(df_artist_top_albums.columns)
print(df_artist_top_tracks.columns)


st.markdown("""
# Chart Exploration
""")

import pandas as pd
from datetime import datetime, timezone

def week_windows_2025_utc():
    # Mondays covering 2025, plus one extra week start
    starts = pd.date_range("2024-12-30", "2026-01-05", freq="W-MON", tz="UTC")
    # Keep only weeks that overlap 2025
    windows = []
    for i in range(len(starts) - 1):
        start = starts[i]
        end = starts[i + 1]
        # overlap with 2025 calendar year
        if end.date() < datetime(2025,1,1).date() or start.date() > datetime(2025,12,31).date():
            continue
        windows.append({
            "week_start_dt": start,
            "week_end_dt": end,
            "from_ts": int(start.timestamp()),
            "to_ts": int(end.timestamp()),
            "week_start": start.date(),
            "week_end": end.date(),
        })
    return pd.DataFrame(windows)

weeks_2025 = week_windows_2025_utc()
weeks_2025.head(), weeks_2025.tail(), len(weeks_2025)

import os, requests

API_KEY = os.environ["LASTFM_API_KEY"]
BASE = "https://ws.audioscrobbler.com/2.0/"

def lastfm(method, **params):
    # Translate Python-safe names to Last.fm parameter names
    if "from_" in params:
        params["from"] = params.pop("from_")
    if "to_" in params:
        params["to"] = params.pop("to_")

    params.update({
        "method": method.lower(),   # <-- IMPORTANT
        "api_key": API_KEY,
        "format": "json",
    })

    r = requests.get(BASE, params=params, timeout=20)
    if not r.ok:
        print("HTTP:", r.status_code)
        print("URL:", r.url)
        print("Body (first 300 chars):", r.text[:300])
        r.raise_for_status()

    data = r.json()
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(f"Last.fm error {data['error']}: {data.get('message')}")
    return data

# 1) chart list works?
data_list = lastfm("tag.getWeeklyChartList", tag="pop")
print(list(data_list.keys()))

# 2) pull ONE weekly chart window from that list
w = data_list["weeklychartlist"]["chart"][0]
data_week = lastfm("tag.getWeeklyArtistChart", tag="pop", from_=w["from"], to_=w["to"])
print(list(data_week["weeklyartistchart"].keys()))
print("First artist row:", data_week["weeklyartistchart"]["artist"][0])

st.markdown("""
# Co\\-listening Test
""")

st.markdown("""
Co\\-listening similarity is only available at the artist level on a global perspective \\(cannot do Artist similarity based on specific countries' patterns\\)\\. Won't work at the album or track level\\.
""")

import pandas as pd

# -----------------------------
# Artist-level co-listening test
# (Last.fm: artist.getSimilar)
# -----------------------------

SEED_ARTIST = "The Weeknd"   # change me

data = lastfm("artist.getSimilar", artist=SEED_ARTIST, limit=50)
rows = data["similarartists"]["artist"]

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

df_artist_similar = pd.DataFrame([{
    "seed_artist": SEED_ARTIST,
    "similar_artist": a.get("name"),
    "similarity_score": safe_float(a.get("match")),   # 0–1
    "similar_artist_mbid": a.get("mbid") or None,
    "similar_artist_url": a.get("url"),
    # Optional fields that sometimes appear:
    "streamable": a.get("streamable"),
    "image_url": (a.get("image", [{}])[-1] or {}).get("#text") if isinstance(a.get("image"), list) else None,
} for a in rows])

df_artist_similar.head(20)

# Quick sanity checks
df_artist_similar.shape, df_artist_similar["similar_artist"].nunique(), df_artist_similar.sort_values("similarity_score", ascending=False).head(10)
