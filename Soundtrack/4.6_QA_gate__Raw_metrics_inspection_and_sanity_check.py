import streamlit as st
import os, sys

st.set_page_config(page_title="4.6 QA gate_ Raw metrics inspection and sanity check", layout="wide")

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
# I\\. Overview
""")

st.markdown("""
Notebook purpose & scope
""")

st.markdown("""
This notebook focuses on raw Last\\.fm listener and playcount metrics sanity checking, not substantive analysis\\. The goal here is to validate the integrity, coherence, and distributional properties of the metrics before any transformation or reduction, and to document the rationale for downstream decisions \\(log transforms, exclusions, aggregation\\)\\.
""")

st.markdown("""
This is intentionally pre\\-analytic work\\. I am not trying to interpret popularity, draw conclusions, or optimize visuals here — only to answer: “Do these metrics behave in ways that make later analysis defensible?”
""")

st.markdown("""
What this notebook should cover
""")

st.markdown("""
1\\. Coverage and information loss
""")

st.markdown("""
\\- Quantify nulls and missing values at both the album and track \\(wide\\) levels\\.

\\- Explicitly connect missingness to planned reduction rules \\(canonical soundtrack filtering, vote\\_count \\> 500\\)\\.

\\- Track row counts before and after these constraints to make information loss visible and intentional\\.

""")

st.markdown("""
2\\. Raw metric properties \\(non\\-visual first\\)
""")

st.markdown("""
\\- Compute basic descriptive statistics for listeners and playcounts at album and track level\\.

\\- Focus on distribution shape indicators \\(skew, kurtosis\\) rather than interpretive summaries\\.

\\- Assess zero inflation and its implications for transformation choices\\.

""")

st.markdown("""
3\\. Metric coherence checks
""")

st.markdown("""
\\- Validate that album\\-level metrics relate sensibly to aggregated track\\-level metrics \\(e\\.g\\., album listeners vs sum of track listeners\\)\\.

\\- Confirm that listener and playcount metrics move together in raw space \\(monotonicity, not modeling\\)\\.

\\- Flag any anomalies that suggest API mismatch, duplication, or aggregation issues\\.

""")

st.markdown("""
4\\. Visual exploration
""")

st.markdown("""
\\- One or two lightweight raw distributions \\(album \\+ track\\) to demonstrate extreme skew\\.

\\- One raw scatter of listeners vs playcounts \\(album\\-level\\) to confirm basic relationship\\.

""")

st.markdown("""
Note: Visuals here are diagnostic, not presentation\\-ready\\.
""")

st.markdown("""
# I\\. Setup
""")

# Standard library imports
import sys
from datetime import datetime

# Third-party imports
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

# os.chdir("/work") # needed for the read_csvs to find files  # path adjusted for Streamlit

# make sure /work is first on the import path (ahead of site-packages)
# sys.path adjusted: add DATA_DIR so local utils/ can be found
if DATA_DIR not in sys.path:
    sys.path.insert(0, DATA_DIR)

pd.set_option('display.max_columns', None)
pd.set_option("display.width", 200)

# Any visualization using our team_theme
import altair as alt

from utils.viz_theme import enable, sized
enable()    # You need to enable the theme in order for it to work

alt.data_transformers.disable_max_rows()

print("Altair:", alt.__version__)
print("Renderer:", alt.renderers.active)
print("Theme:", alt.themes.active)

# Load the key dataframe

albums_df = pd.read_csv("./pipeline/4.5.Albums_join_everything.csv")
artists_df = pd.read_csv("./pipeline/4.5.Artists_join_everything.csv")
tracks_df = pd.read_csv("./pipeline/4.5.Tracks_join_everything.csv")
wide_df = pd.read_csv("./pipeline/4.5.Wide_join_everything.csv")

st.markdown("""
# II\\. Metrics coverage and information loss
""")

st.markdown("""
This section focuses on metric coverage and information loss, not interpretation\\. We quantify how much of the dataset carries any observable Last\\.fm listener and playcount signal at the album, artist, and track levels, and how that coverage changes after applying the core analytical constraints \\(canonical soundtrack selection and a minimum film vote threshold\\)\\. For this purpose, zero and missing values are treated equivalently as “no usable signal\\.” The goal is to make data retention tradeoffs explicit and to determine whether the proportion of rows without observable signal is small enough to justify simple exclusion, rather than imputation, before moving into distributional analysis and transformation decisions\\.
""")

st.markdown("""
Question: How many rows carry any observable popularity signal \\(non\\-zero metric\\) at all?
""")

st.markdown("""
### II\\.1 Helper functions
""")

st.markdown("""
Before running coverage and modeling analyses, we apply a consistent “analysis\\-ready” filter to the album/wide tables\\. This keeps only canonical soundtracks and optionally enforces a TMDB vote\\_count threshold to focus on films with meaningful audience exposure \\(and to reduce long\\-tail noise\\)\\. We then summarize metric coverage before vs\\. after filtering in a single, readable snapshot table\\.
""")

VOTE_THRESHOLD = 500

def apply_album_wide_filters(df, vote_threshold=500):
    """
    Apply the standard downstream filter used for album- and wide-grained analysis.

    Rationale:
      - Restrict to canonical soundtracks so each film maps to a single representative album.
      - Optionally restrict to films above a vote_count threshold to focus on movies with
        sufficient exposure / engagement in TMDB (reduces long-tail sparsity/noise).

    Parameters
    ----------
    df : pd.DataFrame
        Input table (album_df, wide_df, etc.) that includes:
        - is_canonical_soundtrack (0/1)
        - film_vote_count (numeric)
    vote_threshold : int, default=500
        Minimum TMDB vote_count required to keep a row.

    Returns
    -------
    pd.DataFrame
        Filtered dataframe containing only canonical soundtracks with film_vote_count > threshold.
    """
    # Keep only canonical soundtrack rows
    canon = df.loc[df["is_canonical_soundtrack"] == 1].copy()

    # Further restrict to films with sufficient vote_count exposure
    canon_vote = canon.loc[canon["film_vote_count"] > vote_threshold].copy()

    return canon_vote


def coverage_table_two_snapshots(
    df_base,
    df_post,
    metrics,
    label_base="baseline",
    label_post="canonical + vote>500"
):
    """
    Build a compact before/after coverage table for one or more metrics.

    Definition of "covered":
      - A metric value is considered covered if it is NON-null and NON-zero.
      - We intentionally treat NaN and 0 the same (not covered), since Last.fm metrics
        are expected to be positive when present. This keeps the table easy to interpret.

    Output:
      - rows  = metric names
      - cols  = {label_base, label_post}
      - values formatted as: "covered_count / total_rows (pct)"

    Parameters
    ----------
    df_base : pd.DataFrame
        Baseline dataframe (pre-filter).
    df_post : pd.DataFrame
        Post-filter dataframe (e.g., canonical + vote threshold).
    metrics : list[str]
        Column names to compute coverage for (e.g., listeners/playcount fields).
    label_base : str
        Column label for the baseline snapshot.
    label_post : str
        Column label for the post-filter snapshot.

    Returns
    -------
    pd.DataFrame
        Coverage summary indexed by metric.
    """
    def covered_count(df, col):
        """
        Count rows where metric is present (non-null) and non-zero.
        Coerce to numeric defensively so bad strings become NaN -> 0 -> not covered.
        """
        s_num = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return int((s_num != 0).sum())

    base_n = len(df_base)
    post_n = len(df_post)

    out = []
    for m in metrics:
        base_cov = covered_count(df_base, m)
        post_cov = covered_count(df_post, m)

        out.append({
            "metric": m,
            label_base: f"{base_cov:,} / {base_n:,} ({base_cov/base_n:.1%})" if base_n else "NA",
            label_post: f"{post_cov:,} / {post_n:,} ({post_cov/post_n:.1%})" if post_n else "NA",
        })

    return pd.DataFrame(out).set_index("metric")

st.markdown("""
### II\\.2 Album metrics completeness analysis
""")

ALBUM_METRICS = ["lfm_album_listeners", "lfm_album_playcount"]

albums_post = apply_album_wide_filters(albums_df, vote_threshold=VOTE_THRESHOLD)

display(
    coverage_table_two_snapshots(
        df_base=albums_df,
        df_post=albums_post,
        metrics=ALBUM_METRICS,
        label_base="baseline",
        label_post=f"canonical + vote>{VOTE_THRESHOLD}"
    )
)

st.markdown("""
Findings: Album\\-level Last\\.fm coverage is strong at baseline, with ~90% of album rows carrying non\\-zero listener and playcount signals\\. After restricting to canonical soundtracks for films with sufficient vote volume, coverage improves further to ~96%, indicating that the filtering steps reduce noise without materially sacrificing metric availability\\.
""")

ARTIST_METRICS = ["lfm_artist_listeners", "lfm_artist_playcount"]

# empty post snapshot so the function prints NA in the post column
artists_post = artists_df.iloc[0:0].copy()

display(
    coverage_table_two_snapshots(
        df_base=artists_df,
        df_post=artists_post,
        metrics=ARTIST_METRICS,
        label_base="baseline",
        label_post="post-filter (N/A)"
    )
)

st.markdown("""
Findings: Artist\\-level Last\\.fm metrics are nearly complete at baseline \\(~98% coverage\\), suggesting minimal information loss at the artist grain\\. Given the already high coverage, no additional filtering is required for artists prior to downstream aggregation or transformation\\.
""")

st.markdown("""
### I\\.3 Track\\-level metrics completeness analysis
""")

TRACK_METRICS = ["lfm_track_listeners", "lfm_track_playcount"]

wide_post = apply_album_wide_filters(wide_df, vote_threshold=VOTE_THRESHOLD)

display(
    coverage_table_two_snapshots(
        df_base=wide_df,
        df_post=wide_post,
        metrics=TRACK_METRICS,
        label_base="baseline",
        label_post=f"canonical + vote>{VOTE_THRESHOLD}"
    )
)

st.markdown("""
Findings: Track\\-level coverage is high at baseline \\(~92%\\) across listener and playcount metrics, despite the much larger row volume\\. Applying canonical soundtrack and vote\\-count filters both reduces scale and increases coverage to ~95%, reinforcing that these constraints improve data quality while preserving broad track\\-level representation\\.
""")

st.markdown("""
Summary: Because post\\-filter Last\\.fm coverage exceeds 95% at the album and track levels, it looks like rows without observable listener or playcount signals \\(zeros and NaNs\\) can be excluded rather than imputed, avoiding the introduction of synthetic popularity values\\.
""")

st.markdown("""
# III\\. Raw metric statistical properties
""")

st.markdown("""
This section stays in raw Last\\.fm space and focuses on whether the listener and playcount metrics behave in ways that make downstream analysis defensible\\. We compute basic descriptive statistics \\(including tails/percentiles\\) and distribution\\-shape indicators \\(skew and kurtosis\\) at the album, artist, and track levels, and we explicitly quantify zero inflation and missingness\\. The purpose is to justify later preprocessing choices \\(e\\.g\\., log transforms, exclusions, aggregation\\) based on observed distributional properties—not to interpret popularity or draw substantive conclusions\\.
""")

st.markdown("""
Question: Do raw Last\\.fm listener and playcount metrics exhibit distributional properties that require transformation or robustness adjustments before analysis?
""")

VOTE_THRESHOLD = 500

def apply_album_wide_filters(df, vote_threshold=500):
    """
    Standard downstream constraints for albums + wide:
      1) canonical soundtrack rows only
      2) film_vote_count > threshold
    """
    canon = df.loc[df["is_canonical_soundtrack"] == 1].copy()
    canon_vote = canon.loc[canon["film_vote_count"] > vote_threshold].copy()
    return canon_vote

def metric_profile(df, metrics, label, percentiles=(0.25, 0.5, 0.75, 0.90, 0.95, 0.99)):
    rows = []
    n = len(df)

    for m in metrics:
        s = pd.to_numeric(df[m], errors="coerce")
        zero_ct = int((s == 0).sum())
        nonmissing = s.dropna()

        if len(nonmissing) == 0:
            rows.append({
                "dataset": label,
                "metric": m,
                "zero_pct": round(zero_ct / n, 4) if n else np.nan,
                "mean": np.nan, "std": np.nan, "min": np.nan,
                "p25": np.nan, "p50": np.nan, "p75": np.nan,
                "p90": np.nan, "p95": np.nan, "p99": np.nan,
                "max": np.nan,
                "skew": np.nan,
                "kurtosis": np.nan
            })
            continue

        qs = nonmissing.quantile(list(percentiles))
        qmap = {float(p): float(qs.loc[p]) for p in qs.index}

        rows.append({
            "dataset": label,
            "metric": m,
            "zero_pct": (zero_ct / n) if n else np.nan,
            "mean": float(nonmissing.mean()),
            "std": float(nonmissing.std(ddof=1)),
            "min": float(nonmissing.min()),
            "p25": qmap.get(0.25, np.nan),
            "p50": qmap.get(0.50, np.nan),
            "p75": qmap.get(0.75, np.nan),
            "p90": qmap.get(0.90, np.nan),
            "p95": qmap.get(0.95, np.nan),
            "p99": qmap.get(0.99, np.nan),
            "max": float(nonmissing.max()),
            "skew": float(nonmissing.skew()),
            "kurtosis": float(nonmissing.kurt())
        })

    out = pd.DataFrame(rows)

    col_order = [
        "dataset", "metric",
        "zero_pct",
        "mean", "std", "min", "p25", "p50", "p75", "p90", "p95", "p99", "max",
        "skew", "kurtosis"
    ]
    out = out[col_order]

    # Make floats clean (2 decimals) in the output table
    num_cols = out.select_dtypes(include=["number"]).columns
    out[num_cols] = out[num_cols].round(2)

    return out

st.markdown("""
Before the findings, it helps to anchor what these numbers mean:
""")

st.markdown("""
Skew measures asymmetry\\.
""")

st.markdown("""
- Skew ≈ 0 → roughly symmetric
""")

st.markdown("""
- Skew \\> 1 → strongly right\\-skewed \\(long tail of large values\\)
""")

st.markdown("""
- Skew \\> 5 → extreme right\\-skew; most mass is near zero with rare, very large values
""")

st.markdown("""
Kurtosis \\(Fisher kurtosis, which pandas reports\\) measures tail heaviness\\.
""")

st.markdown("""
- Kurtosis ≈ 0 → normal, light\\-tailed
""")

st.markdown("""
- Kurtosis \\> 10 → very heavy\\-tailed
""")

st.markdown("""
- Kurtosis \\> 100 → extreme tail dominance; variance driven by a tiny number of observations
""")

# Metrics to profile
ALBUM_METRICS  = ["lfm_album_listeners", "lfm_album_playcount"]
ARTIST_METRICS = ["lfm_artist_listeners", "lfm_artist_playcount"]
TRACK_METRICS  = ["lfm_track_listeners", "lfm_track_playcount"]

# Post-filter snapshots (albums + wide only)
albums_post = apply_album_wide_filters(albums_df, vote_threshold=VOTE_THRESHOLD)
wide_post   = apply_album_wide_filters(wide_df, vote_threshold=VOTE_THRESHOLD)

# Albums
album_stats = pd.concat([
    metric_profile(albums_df,  ALBUM_METRICS,  label="albums baseline"),
    metric_profile(albums_post, ALBUM_METRICS, label=f"albums canonical + vote>{VOTE_THRESHOLD}")
], axis=0, ignore_index=True)



st.write(album_stats)


st.markdown("""
Findings: Album\\-level Last\\.fm listener and playcount metrics are extremely right\\-skewed and heavy\\-tailed in raw space, both at baseline and after canonical \\+ vote filtering\\. Skew values remain well above 20 and kurtosis remains in the hundreds to thousands, indicating that a small number of blockbuster soundtracks dominate the upper tail while most albums cluster at very low values\\.
""")

st.markdown("""
Applying canonical soundtrack and vote\\-count filters reduces zero rates and slightly lowers skew and kurtosis, but does not materially change the overall distributional shape\\. Medians remain low \\(listeners ~17–24; playcounts ~207–351\\) while maxima reach ~1\\.0 M listeners and ~18\\.3 M plays, reinforcing that raw\\-space summaries are dominated by outliers and motivating scale compression or robust treatment in subsequent analysis\\.
""")

# Artists (no post-filter)
artist_stats = metric_profile(artists_df, ARTIST_METRICS, label="artists baseline")
st.write(artist_stats)

st.markdown("""
Findings: Artist\\-level Last\\.fm metrics are also strongly heavy\\-tailed, especially for playcount\\. Listener counts show substantial right\\-skew \\(skew ~5\\.63\\) with a long upper tail \\(p99 ~3\\.39M; max ~7\\.57M\\), while playcounts are even more concentrated \\(skew ~14\\.55; kurtosis ~258\\) with extreme separation between the median \\(~74K\\) and the maximum \\(~1\\.52B\\)\\. Unlike albums/tracks, zero inflation isn’t a factor here \\(zero\\_pct = 0\\), so the main takeaway at the artist grain is tail dominance rather than coverage loss\\.
""")

# Wide / tracks
wide_stats = pd.concat([
    metric_profile(wide_df,   TRACK_METRICS, label="wide baseline"),
    metric_profile(wide_post, TRACK_METRICS, label=f"wide canonical + vote>{VOTE_THRESHOLD}")
], axis=0, ignore_index=True)
st.write(wide_stats)

st.markdown("""
Findings:  Track\\-level Last\\.fm listener and playcount metrics are extremely right\\-skewed and heavy\\-tailed at both baseline and after canonical \\+ vote filtering\\. Skew remains very high \\(listeners ~17–18; playcounts ~23–27\\) and kurtosis remains in the hundreds to over a thousand, indicating that a small fraction of tracks account for a disproportionate share of total engagement\\.
""")

st.markdown("""
Applying canonical soundtrack and vote\\-count filters raises central values \\(medians and upper percentiles\\) and modestly reduces skew and kurtosis, but does not materially change the underlying distributional shape\\. Zero inflation is not present at the track level \\(zero\\_pct = 0\\), reinforcing that the dominant issue is tail concentration rather than coverage, and that raw\\-space summaries will be driven by extreme outliers without scale compression or robust treatment\\.
""")

st.markdown("""
# IV\\. Metric coherence check
""")

st.markdown("""
This section evaluates metric coherence rather than numeric equivalence\\. Last\\.fm album\\-level and track\\-level listener and playcount metrics are computed using different counting conventions and are not expected to sum or reconcile exactly across levels\\. The purpose of this analysis is therefore not to test additivity, but to assess whether these metrics move together in a consistent and interpretable way across aggregation levels and measures\\.
""")

st.markdown("""
We examine how album\\-level metrics relate to simple aggregations of their constituent tracks \\(e\\.g\\., sums or maxima\\) to understand scale separation and dispersion, and we test whether listener and playcount metrics exhibit monotonic alignment in raw space\\. Large or systematic departures from these expectations are used as diagnostic signals for potential API mismatches, duplication, or aggregation artifacts, rather than as evidence of analytical failure\\.
""")

st.markdown("""
### IV\\.1 Rank coherence
""")

st.markdown("""
Question: Do related Last\\.fm metrics move together in a consistent and interpretable way across levels of aggregation, even when they are not expected to be numerically additive?
""")

# Metric coherence checks need album-level values and track-level aggregates
# on the *same* album key (release_group_mbid).
#
# IMPORTANT:
# - Last.fm album metrics and track metrics are not expected to be additive.
# - We are not testing whether sums "match".
# - We are building a comparison frame so we can test *rank coherence*:
#   Do albums with higher album-level listeners/playcounts also tend to have
#   higher aggregated track-level listeners/playcounts?

# 1) Aggregate track-level metrics up to the album key (release_group_mbid)
#    We compute both SUM and MAX because:
#    - SUM: rough "total track activity" proxy (not expected to equal album metric)
#    - MAX: "top track dominates" proxy (often more stable under heavy tails)
track_agg = (
    wide_df
    .groupby("release_group_mbid", as_index=False)
    .agg(
        track_listener_sum=("lfm_track_listeners", "sum"),
        track_listener_max=("lfm_track_listeners", "max"),
        track_playcount_sum=("lfm_track_playcount", "sum"),
        track_playcount_max=("lfm_track_playcount", "max"),
        track_ct=("track_id", "nunique")  # number of unique tracks contributing
    )
)

# 2) Join album-level metrics to track-level aggregates
#    We use an INNER JOIN so we only keep albums that have track data available.
album_coherence = (
    albums_df[[
        "release_group_mbid",
        "lfm_album_listeners",
        "lfm_album_playcount"
    ]]
    .merge(track_agg, on="release_group_mbid", how="inner")
)

# 3) Quick sanity peek: this is the dataset used for rank/ratio diagnostics
st.write(album_coherence.head(10))

def spearman_check(x, y, label):
    df = (
        pd.DataFrame({"x": x, "y": y})
        .dropna()
    )

    if df.shape[0] < 10:
        print(f"{label}: not enough valid pairs ({df.shape[0]})")
        return

    r, p = spearmanr(df["x"], df["y"])
    print(f"{label}: Spearman r = {r:.3f} (p={p:.2e}, n={df.shape[0]})")

spearman_check(
    album_coherence["lfm_album_listeners"],
    album_coherence["track_listener_sum"],
    "Album listeners vs SUM(track listeners)"
)

spearman_check(
    album_coherence["lfm_album_listeners"],
    album_coherence["track_listener_max"],
    "Album listeners vs MAX(track listeners)"
)

spearman_check(
    album_coherence["lfm_album_playcount"],
    album_coherence["track_playcount_sum"],
    "Album playcount vs SUM(track playcount)"
)

spearman_check(
    album_coherence["lfm_album_playcount"],
    album_coherence["track_playcount_max"],
    "Album playcount vs MAX(track playcount)"
)

st.markdown("""
Findings: Album\\-level and track\\-level Last\\.fm metrics are related, but only loosely\\. The rank correlations fall between 0\\.39 and 0\\.45, which means that albums with higher listener or playcount values at the album level tend to also rank higher on aggregated track\\-level engagement, but with substantial variation\\. Put simply, the metrics usually move in the same direction, but they do not move together tightly\\.
""")

st.markdown("""
This is exactly what we would expect given how Last\\.fm computes these metrics\\. Album\\-level and track\\-level values are generated through different counting processes and are not designed to match numerically\\. A very strong or near\\-perfect relationship would therefore be surprising\\. The fact that the correlations are positive and highly statistically significant indicates that album and track data are correctly linked, while the moderate size of the correlations shows that they capture related but not interchangeable signals\\.
""")

st.markdown("""
Notably, the correlations are similar whether track engagement is aggregated using sums or maxima\\. This suggests that overall track\\-level activity scales with album popularity in a broadly consistent way, without implying that track\\-level metrics are dominated by a single listening pattern or that album\\- and track\\-level engagement measure the same behavior\\. Taken together, these results indicate sufficient coherence between album and track metrics to support downstream analysis, while reinforcing that they should be interpreted as complementary rather than equivalent measures\\.
""")

st.markdown("""
### IV\\.2 Scale ratio analysis between album and track metrics
""")

st.markdown("""
Motivation: In test IV\\.1, we found that album\\-level and track\\-level Last\\.fm metrics are related but not additive\\. Track listeners are counted independently per track, so summing track listeners inflates totals and makes direct scale comparisons misleading\\. To better understand how far apart album\\- and track\\-level engagement typically are without multi\\-counting the same listeners across tracks, we use the maximum track value as a reference point\\. This allows us to compare album\\-level engagement to the scale of an album’s most popular track, producing ratios that are interpretable as measures of relative separation rather than expected equality\\.
""")

st.markdown("""
Question: How far apart are album\\-level and track\\-level popularity measures in practice, and how large is the typical scale gap between an album and its most popular track?
""")

# ------------------------------------------------------------
# Ratio diagnostics (coherence, not additivity):
# How far apart are album-level metrics and track-level metrics in scale?
#
# IMPORTANT:
# - Album and track metrics are not expected to be numerically equivalent.
# - Track listeners are counted independently per track (no cross-track deduplication).
# - We use MAX(track value) to avoid multi-counting and to anchor comparisons
#   to the album’s most popular track.
#
# Ratios are computed only where both values exist and the album metric > 0,
# to avoid divide-by-zero and NaN/inf pollution in summary statistics.
# ------------------------------------------------------------

# Listener ratio:
#   MAX(track listeners) / album listeners
# Interpreted as: how large the album’s most-listened track is
# relative to the album’s overall listener count.
mask = (
    album_coherence["track_listener_max"].notna() &
    album_coherence["lfm_album_listeners"].notna() &
    (album_coherence["lfm_album_listeners"] > 0)
)
album_coherence.loc[mask, "listener_max_ratio"] = (
    album_coherence.loc[mask, "track_listener_max"] /
    album_coherence.loc[mask, "lfm_album_listeners"]
    )

# Playcount ratio:
#   MAX(track playcount) / album playcount
# Interpreted as: how concentrated total plays are in the album’s
# most-played track relative to album-level play activity.
mask = (
    album_coherence["track_playcount_max"].notna() &
    album_coherence["lfm_album_playcount"].notna() &
    (album_coherence["lfm_album_playcount"] > 0)
)
album_coherence.loc[mask, "playcount_max_ratio"] = round(
    album_coherence.loc[mask, "track_playcount_max"] /
    album_coherence.loc[mask, "lfm_album_playcount"]
    )

# Summarize ratio distributions (valid rows only)
# These summaries describe typical separation in scale,
# not agreement or additivity.
ratio_summary = (
    album_coherence[["listener_max_ratio", "playcount_max_ratio"]]
    .dropna()
    .describe(percentiles=[0.5, 0.75, 0.9, 0.95, 0.99])
    .round(2)
)

st.write(ratio_summary)

st.markdown("""
Explanation: These max ratios compare album\\-level engagement to the scale of an album’s most popular track on a per\\-album basis \\(for example, max\\(track listeners\\) ÷ album listeners\\)\\. The goal is not to test whether album and track metrics “match,” but to understand how far apart the two measures typically are in scale\\. A ratio of 1 would mean the album’s most\\-listened track has roughly the same listener count as the album itself; values well above 1 indicate that track\\-level engagement operates at a much larger scale than album\\-level counts\\.
""")

st.markdown("""
Findings: The ratios between track\\-level maxima and album\\-level metrics span multiple orders of magnitude, confirming substantial and expected scale separation between Last\\.fm’s album\\- and track\\-level counts\\. For a typical album, the median most\\-listened track has about 17× as many listeners as the album\\-level listener count, while the median most\\-played track has about 6× the album\\-level playcount\\. This shows that even in central cases, track\\-level engagement tends to exceed album\\-level engagement by a wide margin\\.
""")

st.markdown("""
The upper tail of the distribution is extremely wide\\. By the 75th percentile, ratios already reach into the hundreds, and by the 90th–95th percentiles they climb into the thousands\\. At the very top of the distribution, ratios grow by additional orders of magnitude\\. This indicates that for a subset of albums, a single track attracts attention on a scale that far exceeds album\\-level interactions, rather than album engagement scaling proportionally with track popularity\\.
""")

st.markdown("""
Crucially, these ratios do not cluster around a stable multiplier, and no consistent conversion emerges between album\\-level and track\\-level metrics\\. Instead, the scale gap varies widely across albums\\. This confirms that while album\\- and track\\-level metrics are directionally related, they represent distinct views of popularity and should be analyzed as complementary signals rather than treated as interchangeable or reducible to a single scale\\.
""")

st.markdown("""
### IV\\.3 Monotonicity within entities
""")

st.markdown("""
Question: Do listener counts and playcounts move together consistently, such that higher listener values are associated with higher playcounts at the same level of analysis?
""")

# Album-level
spearman_check(
    albums_df["lfm_album_listeners"],
    albums_df["lfm_album_playcount"],
    "Album listeners vs album playcount"
)

# Track-level
spearman_check(
    wide_df["lfm_track_listeners"],
    wide_df["lfm_track_playcount"],
    "Track listeners vs track playcount"
)

st.markdown("""
Finding: Listener and playcount metrics exhibit very strong monotonic alignment at both the album and track levels\\. Album\\-level listeners and playcounts are almost perfectly rank\\-aligned \\(Spearman ρ = 0\\.96\\), and track\\-level listeners and playcounts show similarly strong alignment \\(Spearman ρ = 0\\.98\\)\\. This indicates that, despite differences in scale and distribution, the two metrics provide a consistent ordering of popularity and can be treated as coherent alternative measures of engagement rather than conflicting signals\\.
""")

st.markdown("""
Summary\\. Album\\- and track\\-level Last\\.fm metrics are directionally coherent but not numerically reconcilable\\. Rank\\-based checks show moderate alignment between album\\- and track\\-level popularity, while listener and playcount metrics exhibit near\\-perfect monotonic alignment within each level\\. Scale ratio diagnostics reveal large and highly variable gaps between album\\- and track\\-level counts with no stable mapping between them\\. Taken together, these results indicate that album\\- and track\\-level popularity should be analyzed separately rather than treated as interchangeable\\.
""")

st.markdown("""
Based on these findings, we adopt the following modeling choices:
""")

st.markdown("""
Album\\-level analysis: Use listeners to reflect the breadth of engagement with a soundtrack, aligning naturally with film\\- and album\\-level predictors\\.
""")

st.markdown("""
Track\\-level analysis: Use playcounts to capture listening intensity and within\\-album variation, providing a richer signal for track\\-level modeling\\.
""")

st.markdown("""
# V\\. Variance decomposition
""")

st.markdown("""

Let's dig deeper to see where the variability in popularity lives \\(between albums vs within albums\\), because it supports decisions about aggregation and modeling without being part of the behavioral story\\.

""")

st.markdown("""
Question: How much variance in log popularity is between albums vs within albums?

""")

# Guard against weird negatives (shouldn't happen, but keeps logs safe)
wide_df["lfm_track_listeners"] = wide_df["lfm_track_listeners"].where(wide_df["lfm_track_listeners"] >= 0, np.nan)
wide_df["lfm_track_playcount"] = wide_df["lfm_track_playcount"].where(wide_df["lfm_track_playcount"] >= 0, np.nan)

wide_df["log_track_listeners"]  = np.log1p(wide_df["lfm_track_listeners"])
wide_df["log_track_playcounts"] = np.log1p(wide_df["lfm_track_playcount"])

st.markdown("""
This helper decomposes track\\-level variability into two intuitive pieces: how much of the spread comes from differences between albums \\(some albums are consistently higher/lower\\), versus differences within an album \\(tracks on the same album vary a lot\\)\\. It’s a quick way to sanity\\-check whether a metric behaves more like an album attribute or a track attribute\\.
""")

def decomp_by_album(df, value_col):
    # Keep only the keys we need plus the metric we're decomposing, and drop incomplete rows
    d = (
        df[["release_group_mbid", "track_id", value_col]]
        .dropna(subset=["release_group_mbid", "track_id", value_col])
    )

    # Total variance across all track rows (population variance: ddof=0)
    total_var = d[value_col].var(ddof=0)

    # Album-level mean of the metric (one mean per release_group)
    album_mean = d.groupby("release_group_mbid")[value_col].mean()

    # Track count per album (used as weights so albums with more tracks matter more)
    n_tracks = d.groupby("release_group_mbid").size()

    # Weighted overall mean across albums (equivalent to track-weighted mean)
    overall_mean = np.average(album_mean, weights=n_tracks)

    # Between-album variance: how much album means vary around the overall mean (track-weighted)
    between_var = np.average((album_mean - overall_mean) ** 2, weights=n_tracks)

    # Attach each track’s album mean back onto the track-level table
    d = d.join(album_mean.rename("album_mean"), on="release_group_mbid")

    # Within-album variance: average squared deviation of tracks from their album mean
    within_var = ((d[value_col] - d["album_mean"]) ** 2).mean()

    # Return a compact summary (and the variance shares for quick interpretation)
    return {
        "value_col": value_col,
        "n_tracks_total": int(len(d)),
        "n_albums": int(n_tracks.shape[0]),
        "total_var": round(float(total_var), 2),
        "between_album_var": round(float(between_var), 2),
        "within_album_var": round(float(within_var), 2),
        "between_share_%": round(float(between_var / total_var), 2),
        "within_share_%": round(float(within_var / total_var), 2),
    }

out = pd.DataFrame([
    decomp_by_album(wide_df, "log_track_listeners"),
    decomp_by_album(wide_df, "log_track_playcounts"),
]).set_index("value_col").T

row_order = [
    "n_tracks_total", "n_albums",
    "total_var", "between_album_var", "within_album_var",
    "between_share_%", "within_share_%",
]
out = out.loc[row_order]

# auto-detected possible Altair chart: out
try:
    st.altair_chart(out, use_container_width=True)
except Exception:
    st.write(out)

st.markdown("""
Findings:  Across ~72K tracks spanning ~4\\.6K soundtrack albums, most of the variation in track popularity is driven by which album a track belongs to, not by differences between tracks within the same album\\. For both log listeners and log playcounts, roughly 69% of total variance sits between albums, indicating that album\\-level context establishes a strong baseline for popularity\\.
""")

st.markdown("""
At the same time, the remaining ~31% of variance occurs within albums, meaning tracks on the same soundtrack still differ meaningfully in how attention is distributed\\. Album aggregation therefore captures the dominant signal, but it does not collapse listening behavior into a single uniform outcome\\.
""")

st.markdown("""
The takeaway is that album context sets the baseline for popularity, but it does not determine how attention is allocated within an album, which is why both album\\-level summaries and within\\-album \\(track\\-level\\) structure are worth examining\\.
""")

st.markdown("""
# VI\\. Visual exploration
""")

st.markdown("""
### VI\\.1 Raw distribution histograms
""")

st.markdown("""
Question: What is the distribution of album listeners and playcounts? How right\\-skewed are the raw numbers?
""")

# Raw distribution of listeners in album

# We cannot pass all of albums_df into Altair or it will blow past
# Deepnote's memory limits
album_listener_df = albums_df[['lfm_album_listeners']]

alt.Chart(album_listener_df) \
    .mark_bar().encode(
    alt.X('lfm_album_listeners:Q', bin = alt.Bin(maxbins = 40),
    ),
    y='count()'
).properties(
    title={
        "text": "Album listeners (raw)",
        "subtitle": ["Extreme skew causes most observations",
        "to collapse into the first bin"],
        "subtitleFontSize": 11
    }
)

st.markdown("""
The histogram is dominated by the first bin\\. This shows you the degree of skew that's present in the album listener distribution\\! Let's see if we can get a better sense of the shape if we log transform\\.
""")

alt.Chart(album_listener_df).transform_filter(
    alt.datum.lfm_album_listeners > 0
).transform_calculate(
    log_album_listeners = "log(1 + datum.lfm_album_listeners)") \
    .mark_bar().encode(
        x = alt.X("log_album_listeners:Q", bin = alt.Bin(maxbins = 40)),
        y = "count()",
        ).properties(
            title={
                "text": "Album listeners (log)",
                "subtitle": ["Log transform reveals the",
                "underlying distribution shape"],
                "subtitleFontSize": 11
                }
                )

st.markdown("""
Now let's take a look at the distribution of track playcounts\\. We'll start with the raw counts, though we are pessimistic about what using the raw counts\\.\\.\\.
""")

# Isolate the track playcount metric
track_playcount_df = wide_df[['lfm_track_playcount']]

alt.Chart(track_playcount_df).mark_bar().encode(
    x = alt.X("lfm_track_playcount:Q", bin = alt.Bin(maxbins = 40)),
    y = "count()"
    ).properties(
        title = {
            "text": "Track playcount (raw)",
            "subtitle": ["Raw distribution collapses into",
            "the first bin due to extreme right-skew"],
            "subtitleFontSize": 11
        }
    )

st.markdown("""
As expected, we get the same extreme\\-skew histogram when we plot the raw track playcount numbers\\. We need to do a similar transformation to view the log distribution histogram for track playcounts\\.
""")

chart = (
    alt.Chart(track_playcount_df)
    .transform_filter(alt.datum.lfm_track_playcount > 0)
    .transform_calculate(log_track_playcount="log(datum.lfm_track_playcount)")
    .mark_bar()
    .encode(
        x=alt.X("log_track_playcount:Q", bin=alt.Bin(maxbins=40), title="Track playcount (log)"),
        y=alt.Y("count():Q", title="Count")
    )
    .properties(
        title={
            "text": "Track playcount (log)",
            "subtitle": [
                "Track playcounts concentrate around a single mode",
                "with a persistent right tail"
            ],
            "subtitleFontSize": 11
        }
    )
)
# auto-detected possible Altair chart: chart
try:
    st.altair_chart(chart, use_container_width=True)
except Exception:
    st.write(chart)

st.markdown("""
Findings: Visual exploration confirms that both album listeners and track playcounts exhibit extreme right\\-skew on the raw scale, with most observations concentrated near zero and a small number of highly popular items dominating the upper tail\\. 
""")

st.markdown("""
On the log scale, the underlying structure becomes visible: album listeners retain a long right tail, while track playcounts form a more concentrated, unimodal distribution with a gradual tail\\. Together, these views show that the apparent irregularity in the raw metrics is driven primarily by scale rather than noise, motivating log transformation for downstream analysis while preserving the raw distributions for reference\\.
""")

st.markdown("""
### VI\\.2 Scatterplot of listener and playcounts
""")

st.markdown("""
Question: If we plot listener and playcounts, will we see an obvious relationship? Will film year be a possible confounding variable?
""")

source = albums_df[['lfm_album_listeners', 'lfm_album_playcount', 'film_year', 'album_title']]

chart = alt.Chart(source).mark_circle(
    opacity=0.35,
    size=40
).encode(
    # Use raw values + log axis scaling (log10-style ticks like 1, 10, 100, 1,000...)
    x=alt.X(
        "lfm_album_listeners:Q",
        title="Album listeners (log10 scale)",
        scale=alt.Scale(type="log")
    ),
    y=alt.Y(
        "lfm_album_playcount:Q",
        title="Album playcount (log10 scale)",
        scale=alt.Scale(type="log")
    ),
    color=alt.Color(
        "film_year:O",
        title="Release year",
        legend=alt.Legend(format="d")
    ),
    tooltip=[
        alt.Tooltip("album_title:N", title="Album"),
        alt.Tooltip("lfm_album_listeners:Q", title="Album listeners", format=","),
        alt.Tooltip("lfm_album_playcount:Q", title="Album playcount", format=","),
        alt.Tooltip("film_year:O", title="Release year", format="d")
    ]
).transform_filter(
    # Log scales can't display 0/negative values
    "datum.lfm_album_listeners > 0 && datum.lfm_album_playcount > 0"
).properties(
    width=750,
    height=400,
    title={
        "text": "Album popularity metrics by year",
        "subtitle": [
            "Listeners and playcounts move together across albums.",
            "No visible clustering by release year."
        ]
    }
)

d = chart.to_dict()
print("Has title:", "title" in d)
print("X title:", d["encoding"]["x"].get("title"))
print("Y title:", d["encoding"]["y"].get("title"))
print("Legend title:", d["encoding"]["color"].get("title"))

# auto-detected possible Altair chart: chart
try:
    st.altair_chart(chart, use_container_width=True)
except Exception:
    st.write(chart)

st.markdown("""
Findings: Album listeners and album playcounts exhibit a strong, monotonic relationship on the log–log scale, indicating that albums with larger audiences also tend to accumulate proportionally more total plays\\. There is no clear separation between albums driven primarily by breadth versus depth; instead, overall popularity appears to scale along a single dominant dimension that reflects both audience size and repeat listening together\\.
""")

st.markdown("""
Release year is a natural candidate confounder here — newer albums might behave differently — but coloring by year shows no meaningful separation or distortion of the relationship\\. Albums from different years all fall along the same curve, indicating that year does not confound the listener–playcount relationship and primarily affects scale rather than structure\\.
""")

source = wide_df[['lfm_track_listeners', 'lfm_track_playcount', 'film_year', 'album_title', 'track_title']]

alt.Chart(source).mark_circle(
    opacity=0.35,
    size=40
).encode(
    # Use raw values + log axis scaling (log10-style ticks like 1, 10, 100, 1,000...)
    x=alt.X(
        "lfm_track_listeners:Q",
        title="Track listeners (log10 scale)",
        scale=alt.Scale(type="log")
    ),
    y=alt.Y(
        "lfm_track_playcount:Q",
        title="Track playcount (log10 scale)",
        scale=alt.Scale(type="log")
    ),
    color=alt.Color(
        "film_year:O",
        title="Release year",
        legend=alt.Legend(format="d")
    ),
    tooltip=[
        alt.Tooltip("album_title:N", title="Album"),
        alt.Tooltip("track_title:N", title="Track"),
        alt.Tooltip("lfm_track_listeners:Q", title="Track listeners", format=","),
        alt.Tooltip("lfm_track_playcount:Q", title="Track playcount", format=","),
        alt.Tooltip("film_year:O", title="Release year", format="d")
    ]
).transform_filter(
    # Log scales can't display 0/negative values
    "datum.lfm_track_listeners > 0 && datum.lfm_track_playcount > 0"
).properties(
    width=750,
    height=400,
    title={
        "text": "Track popularity metrics by year",
        "subtitle": [
            "Track listeners and playcounts exhibit a strong monotonic relationship",
            "No visible clustering emerges when the relationship is stratified by release year"
        ]
    }
)

st.markdown("""
Findings: As expected, track listeners and track playcounts follow a strong, monotonic relationship when viewed on the log scale\\. Coloring by release year does not reveal clustering or deviations from this pattern, indicating that the relationship is stable across time and not driven by specific release cohorts\\.
""")

st.markdown("""
### VI\\.3 Album listeners vs sum of tracks
""")

st.markdown("""
We established earlier that album listeners were far smaller than the sum of its tracks' listeners as justification for splitting out the analysis between albums and tracks\\. Now let's visualize it\\.
""")

st.markdown("""
Question:  How closely does album\\-level listener reach align with aggregated track\\-level listener reach for the same album?
""")

"""
Build an album-level scatterplot comparing overall album reach vs. the reach of its single most-listened track.

Why this exists:
- wide_df is track-grained (many rows per album). This viz needs one row per album.
- We collapse tracks -> album summary stats, then plot:
    x = Last.fm album listeners
    y = max Last.fm track listeners (within that album)
- Both axes are log-scaled, so we filter out non-positive values.
- We color by the ratio (top track listeners / album listeners) to highlight albums
  dominated by a breakout track versus albums with more evenly distributed listening.
"""

# Pull only the fields needed for this visualization (keep it lightweight)
source = wide_df[[
    'lfm_album_listeners',
    'lfm_track_listeners',
    'release_group_id',
    'film_year',
    'album_title'
]]

# ------------------------------------------------------------
# 1) Collapse track rows → one row per album (release_group_id)
#    This dataframe exists ONLY to support this visualization.
#
# Notes:
# - album_listeners is constant at the album level, but repeated across track rows in wide_df,
#   so we take max() as a safe way to recover the album-level value.
# - max_track_listeners captures the single most listened-to track on the album.
# - track_ct is a quick sanity check (how many track rows contributed to this album summary).
# ------------------------------------------------------------
album_viz_df = (
    source
    .groupby('release_group_id', as_index=False)
    .agg(
        album_listeners=('lfm_album_listeners', 'max'),
        max_track_listeners=('lfm_track_listeners', 'max'),
        track_ct=('lfm_track_listeners', 'count'),
        film_year=('film_year', 'max'),
        album_title=('album_title', 'max'),
    )
)

# ------------------------------------------------------------
# 2) Log scales cannot display 0 or negative values → filter them out
#    (Also helps avoid divide-by-zero in the ratio step.)
# ------------------------------------------------------------
album_viz_df = album_viz_df[
    (album_viz_df['album_listeners'] > 0) &
    (album_viz_df['max_track_listeners'] > 0)
].copy()

# ------------------------------------------------------------
# 3) Compute dominance ratio + bucket for color encoding
#    ratio > 1 means the top track outperforms the album-level listeners.
#    Buckets make the legend readable vs. a continuous color scale.
# ------------------------------------------------------------
album_viz_df['ratio'] = (
    album_viz_df['max_track_listeners'] /
    album_viz_df['album_listeners']
)

# Bin the top-track/album ratio into intuitive dominance tiers for coloring and legend clarity
album_viz_df['ratio_bucket'] = pd.cut(
    album_viz_df['ratio'],
    bins=[-float("inf"), 1, 2, 5, 10, float("inf")],
    labels=['<1x', '1–2x', '2–5x', '5–10x', '10x+']
)


# ------------------------------------------------------------
# 4) Build a y = x reference line (parity line) for the log-log plot
#    We use the overlapping domain so the line stays visible on both axes.
# ------------------------------------------------------------
line_min = max(
    album_viz_df['album_listeners'].min(),
    album_viz_df['max_track_listeners'].min()
)
line_max = min(
    album_viz_df['album_listeners'].max(),
    album_viz_df['max_track_listeners'].max()
)

diagline_df = pd.DataFrame({'v': [line_min, line_max]})

diagline = (
    alt.Chart(diagline_df)
    .mark_line(strokeDash=[8, 8], strokeWidth=4, opacity=0.95)
    .encode(
        x=alt.X('v:Q', scale=alt.Scale(type='log')),
        y=alt.Y('v:Q', scale=alt.Scale(type='log'))
    )
)

# ------------------------------------------------------------
# 5) Scatter plot: each point is one album
#    x = album listeners, y = listeners of the album's top track
# ------------------------------------------------------------
points = (
    alt.Chart(album_viz_df)
    .mark_circle(opacity=0.65, size=35)
    .encode(
        x=alt.X(
            'album_listeners:Q',
            title='Album listeners',
            scale=alt.Scale(type='log')
        ),
        y=alt.Y(
            'max_track_listeners:Q',
            title='Listeners (most-listened track)',
            scale=alt.Scale(type='log')
        ),
        color=alt.Color(
            'ratio_bucket:N',
            title='Top track / album',
            sort=['<1x', '1–2x', '2–5x', '5–10x', '10x+']
        ),
        tooltip=[
            alt.Tooltip("album_title:N", title="Album"),
            alt.Tooltip("film_year:O", title="Release year", format="d"),
            alt.Tooltip("track_ct:Q", title="Tracks in album", format=","),  # helpful QA context
            alt.Tooltip("album_listeners:Q", title="Album listeners", format=","),
            alt.Tooltip("max_track_listeners:Q", title="Top track listeners", format=","),
            alt.Tooltip("ratio:Q", title="Ratio", format=".2f"),
            alt.Tooltip("ratio_bucket:N", title="Ratio bucket")
        ]
    )
)

# ------------------------------------------------------------
# 6) Final chart: layer the scatter + parity line
# ------------------------------------------------------------
chart = alt.layer(points, diagline).properties(
    width=750,
    height=400,
    title=alt.TitleParams(
        text="Album listeners vs. max of track listeners",
        subtitle=[
            "Each point is one album (release_group_id).",
            "Dashed line indicates parity (y = x)."
        ],
        fontSize=22,
        subtitleFontSize=14
    )
).configure_axis(
    labelColor="#111111",
    titleColor="#111111"
).configure_legend(
    labelColor="#111111",
    titleColor="#111111"
).configure_title(
    color="#111111",
    subtitleColor="#5E17A6"
)

# auto-detected possible Altair chart: chart
try:
    st.altair_chart(chart, use_container_width=True)
except Exception:
    st.write(chart)

st.markdown("""
Findings: This chart illustrates why album\\-level and track\\-level listener counts shouldn’t be treated as interchangeable\\. While some albums sit near—or even below—the y = x line, many have a most\\-listened track whose listener count far exceeds the album’s listener total, and the size of that gap varies widely from album to album\\. That spread is especially pronounced among lower\\-listener albums, where track\\-level reach can diverge sharply from album\\-level interest\\. The overall pattern makes it clear that album listeners and track listeners reflect different listening behaviors, which is why collapsing one level into the other risks distorting how engagement is interpreted\\.
""")
