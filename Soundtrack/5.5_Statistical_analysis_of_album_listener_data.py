import streamlit as st
import os, sys

st.set_page_config(page_title="5.5 Statistical analysis of album listener data", layout="wide")

# ---------------------------------------------------------------------------
# Data files live next to this script (or in pipeline/ / Soundtrack/ sub-dirs).
# Adjust DATA_DIR if you deploy with a different layout.
# ---------------------------------------------------------------------------
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# pip install "altair<6"

st.markdown("""
# I\\. Setup and normalization
""")

# Imports
import pandas as pd
import numpy as np
import sys
import altair as alt
from datetime import datetime
from scipy.stats import spearmanr, ttest_ind
import statsmodels.api as sm

# os.chdir("/work") # needed for the read_csvs to find files  # path adjusted for Streamlit

# make sure /work is first on the import path (ahead of site-packages)
# sys.path adjusted: add DATA_DIR so local utils/ can be found
if DATA_DIR not in sys.path:
    sys.path.insert(0, DATA_DIR)

pd.set_option('display.max_columns', None)
pd.set_option("display.width", 200)

import altair as alt
alt.data_transformers.disable_max_rows()

from utils.viz_theme import enable
enable()

# See what themes exist and what's active
print("Themes available:", list(alt.themes.names()))
print("Active theme:", alt.themes.active)

# Load the key dataframe

albums_df = pd.read_csv("./pipeline/4.7.Albums_analytics_set.csv")
wide_df = pd.read_csv("./pipeline/4.7.Wide_analytics_set.csv")

print(albums_df.columns.tolist())
print(wide_df.columns.tolist())

st.dataframe(albums_df.sample(5))

nullcount = albums_df.isna().sum()
print(nullcount.index.tolist())
print(nullcount.tolist())

st.markdown("""
# II\\. Correlation analysis
""")

st.markdown("""
### II\\.1 Setup analytics frame
""")

st.markdown("""
Let's add a few derived features into albums\\_df\\. First we'll add n\\_tracks which counts the number of tracks in an album\\. This will be a new derived feature at the album level\\.
""")


# Add n_tracks: number of tracks

# Count unique tracks per (release_group_mbid, tmdb_id)
# If you have already computed n_tracks before, this will overwrite it deterministically.
n_tracks_df = (
    wide_df[["release_group_mbid", "tmdb_id", "track_id"]]
    .groupby(["release_group_mbid", "tmdb_id"], as_index=False)
    .agg(n_tracks=("track_id", "nunique"))
)

# Drop existing column to avoid _x/_y suffixes on repeated runs
if "n_tracks" in albums_df.columns:
    albums_df = albums_df.drop(columns=["n_tracks"])

albums_df = albums_df.merge(
    n_tracks_df,
    on=["release_group_mbid", "tmdb_id"],
    how="left",
    validate="1:1",
)

print("n_tracks added. Nulls:", int(albums_df["n_tracks"].isna().sum()))
print(albums_df["n_tracks"].describe())

st.markdown("""
These award features were derived after inspecting the correlation matrix for the original award indicators\\. The analysis showed that Oscar, Golden Globe, and Critics Choice variables were correlated with each other and largely reflect the same “U\\.S\\. major award recognition” signal, while BAFTA behaved more independently\\. Based on that, I collapsed the U\\.S\\. awards into combined features but kept score and song separate\\. For modeling, I use nominee counts only \\(rather than also including winner counts\\) because winners are essentially a subset of nominees and including both in the same regression would be redundant and can make coefficients unstable\\. BAFTA is left as simple nominee/winner flags since it represents a single ceremony and doesn’t benefit from aggregation\\.
""")

# ------------------------------------------------------------
# Derived award attributes (Option 2: nominees only, everywhere)
# ------------------------------------------------------------

# US-major awards: keep score vs song separate (nominees only)
us_score_nominee_cols = [
    "oscar_score_nominee",
    "globes_score_nominee",
    "critics_score_nominee",
]

us_song_nominee_cols = [
    "oscar_song_nominee",
    "globes_song_nominee",
    "critics_song_nominee",
]

# BAFTA (nominee flag only)
bafta_nominee_col = ["bafta_score_nominee"]

# Derived columns to create
derived_award_cols = [
    "us_score_nominee_count",
    "us_song_nominee_count",
    "bafta_nominee",
]

# Drop derived columns if they already exist
existing = [c for c in derived_award_cols if c in albums_df.columns]
if existing:
    albums_df = albums_df.drop(columns=existing)

# Build US-major nominee counts
albums_df["us_score_nominee_count"] = albums_df[us_score_nominee_cols].sum(axis=1).astype(int)
albums_df["us_song_nominee_count"]  = albums_df[us_song_nominee_cols].sum(axis=1).astype(int)

# Build BAFTA nominee flag
albums_df["bafta_nominee"] = (albums_df[bafta_nominee_col].sum(axis=1) > 0).astype(int)

print("Derived award cols added. Sums:")
print(albums_df[derived_award_cols].sum().sort_values(ascending=False))

st.markdown("""
This step adds a simple “composer experience” feature\\. It counts how many soundtrack albums each composer appears on in our dataset \\(composer\\_album\\_count\\) and merges that value back onto every album row for use in later analysis and modeling\\.
""")

# Add a composer_album_count feature to each album representing the number of albums
# this composer is associated with

composer_counts = (
    albums_df[["composer_primary_clean", "release_group_mbid", "tmdb_id"]]
    .groupby("composer_primary_clean", as_index=False)
    .agg(composer_album_count=("release_group_mbid", "count"))
)

if "composer_album_count" in albums_df.columns:
    albums_df = albums_df.drop(columns=["composer_album_count"])

albums_df = albums_df.merge(
    composer_counts,
    on="composer_primary_clean",
    how="left",
    validate="m:1",
)

print("composer_album_count added. Nulls:", int(albums_df["composer_album_count"].isna().sum()))
print(albums_df["composer_album_count"].describe())

st.markdown("""
Album genre flags currently are stored as objects with values True, False or NaN \\(when no tags were present\\)\\. For the correlation analysis, we should convert these to False
""")


genre_cols = [
    "ambient_experimental", "classical_orchestral", "electronic",
    "hip_hop_rnb", "pop", "rock", "world_folk"
]

# Idempotent: bool/float/object -> int(0/1); NaN -> 0
albums_df[genre_cols] = (
    albums_df[genre_cols]
    .fillna(False)     # NaN -> False
    .astype(bool)      # anything truthy -> True, False stays False
    .astype(int)       # True/False -> 1/0
)

st.markdown("""
This defines the final set of film\\- and album\\-level predictors \\(plus derived award features\\) and builds album\\_analytics\\_df, the modeling\\-ready dataframe we use for diagnostics and regression with log\\_lfm\\_album\\_listeners as the target\\.
""")


film_ids = ["tmdb_id"]

film_features = [
    # --- Exposure / reach ---
    "film_vote_count", "film_popularity",

    # --- Economics (optional in models, but retained) ---
    "film_budget", "film_revenue",

    # --- Quality ---
    "film_rating",

    # --- Timing and Structure ---
    "days_since_film_release","film_runtime_min",

    # --- Film genre indicators ---
    "film_is_action", "film_is_adventure", "film_is_animation", "film_is_comedy", "film_is_crime",
    "film_is_documentary", "film_is_drama", "film_is_family", "film_is_fantasy", "film_is_history",
    "film_is_horror", "film_is_music", "film_is_mystery", "film_is_romance", "film_is_science_fiction",
    "film_is_tv_movie", "film_is_thriller", "film_is_war", "film_is_western"
]

album_ids = [
    "release_group_mbid",
    "tmdb_id",
]

album_features = [
    # --- Core ---
    "days_since_album_release", "n_tracks", "composer_album_count",

    # --- Album genre indicators (0/1) ---
    "ambient_experimental", "classical_orchestral", "electronic", "hip_hop_rnb", "pop", "rock", "world_folk",
]

y_feature = ['log_lfm_album_listeners']

album_analytics_df = albums_df[film_features + album_features + derived_award_cols + y_feature]
# album_analytics_df = albums_df[film_features + album_features + award_cols + y_feature]

st.markdown("""
### II\\.2 Correlation table
""")

st.markdown("""
This section explores pairwise relationships across film, album, genre, award, timing, and composer features using Spearman rank correlations\\. Spearman is used instead of Pearson because many variables in this dataset are binary indicators or heavy\\-tailed measures \\(e\\.g\\., film vote counts, budgets, and revenues\\), and relationships are often monotonic rather than strictly linear\\. Rank\\-based correlations provide a stable view of association structure in this setting without assuming linearity or allowing extreme values to dominate\\.
""")

corr_df = album_analytics_df.select_dtypes(include=["number", "bool"])
print(corr_df.shape, album_analytics_df.shape)   # Verify that we don't lose columns

corr_spearman = corr_df.corr(method="spearman")

# auto-detected possible Altair chart: corr_spearman
try:
    st.altair_chart(corr_spearman, use_container_width=True)
except Exception:
    st.write(corr_spearman)

def corr_to_long(corr_df: pd.DataFrame) -> pd.DataFrame:
    return (
        corr_df
        .reset_index()
        .melt(id_vars="index", var_name="var_y", value_name="corr")
        .rename(columns={"index": "var_x"})
    )

corr_long = corr_to_long(corr_spearman)   # or corr_pearson

heatmap = (
    alt.Chart(corr_long)
    .mark_rect()
    .encode(
        x=alt.X(
            "var_x:O",
            title=None,
            sort=None,
            axis=alt.Axis(labelAngle=-45, labelFontSize=13, titleFontSize=13)
        ),
        y=alt.Y(
            "var_y:O",
            title=None,
            sort=None,
            axis=alt.Axis(labelFontSize=13, titleFontSize=13)
        ),
        color=alt.Color(
            "corr:Q",
            scale=alt.Scale(
                range=["#CC0000", "#F3F0E6", "#1195B2"],
                domain=[-1, 1],
                domainMid=0,
                clamp=True
            ),
            title="Correlation"
        ),
        tooltip=[
            alt.Tooltip("var_x:N", title="X"),
            alt.Tooltip("var_y:N", title="Y"),
            alt.Tooltip("corr:Q", format=".2f", title="ρ"),
        ],
    )
    .properties(
        width=700,
        height=700,
        autosize=alt.AutoSizeParams(type="pad"),
        title="Correlation Heatmap (Spearman)"
    )
)

# auto-detected possible Altair chart: heatmap
try:
    st.altair_chart(heatmap, use_container_width=True)
except Exception:
    st.write(heatmap)

heatmap.to_dict()["height"]
heatmap.to_dict().get("height"), heatmap.to_dict().get("autosize")

st.markdown("""
### II\\.3 Listener\\-focused correlation view \\(Lollipop Plot\\)
""")

st.markdown("""
In the previous section, we used a Spearman correlation heatmap to understand how features relate to one another and to surface clusters of highly collinear variables\\. That view is helpful for diagnosing redundancy, but it’s less useful when the goal is to understand what actually moves with album popularity itself\\. Here, we narrow the focus to a single variable of interest—log\\-transformed album listener counts—and ask a simpler question: which features tend to scale up or down as album popularity increases\\. Because this is a question about relative magnitude rather than rank, we use Pearson correlations on log\\-transformed values, which lets us compare strength and direction of association in a more interpretable way\\.
""")

st.markdown("""
Question:  Which album\\- and film\\-level features tend to scale with soundtrack album popularity?
""")

target = corr_df.columns[-1]

corr_sorted = (
    corr_df
    .corr(method="pearson")[target]   # correlation with target
    .drop(target)                     # remove self-correlation
    .sort_values(key=lambda s: s.abs(), ascending=False)
)

# auto-detected possible Altair chart: corr_sorted
try:
    st.altair_chart(corr_sorted, use_container_width=True)
except Exception:
    st.write(corr_sorted)

st.markdown("""
To get an initial read on which variables move with soundtrack popularity, this lollipop chart ranks features by their Pearson correlation with log\\(album listeners\\)\\. Each row shows one feature’s correlation value: the dot marks the magnitude and direction, while the “stick” connects it back to zero to make weak vs\\. strong relationships easy to compare at a glance\\. A dashed vertical line at 0 highlights features with essentially no linear association; points to the right indicate positive relationships and points to the left indicate negative ones \\(with color reinforcing the direction\\)\\. This is an exploratory, pairwise view—useful for spotting broad patterns and potential predictors—but it does not control for overlap between features the way the regression model does\\.
""")

# Convert series to a dataframe
corr_df_plot = (
    corr_sorted
    .rename("corr")
    .reset_index()
    .rename(columns={"index": "feature"})
)

corr_df_plot["abs_corr"] = corr_df_plot["corr"].abs()
corr_df_plot["zero"] = 0

x_domain = [
    float(corr_df_plot["corr"].min()) - 0.02,
    float(corr_df_plot["corr"].max()) + 0.02
]

x_axis = alt.X(
    "corr:Q",
    title="Correlation with log(album listeners)",
    scale=alt.Scale(domain=x_domain)
)

y_axis = alt.Y(
    "feature:N",
    sort=alt.SortField("abs_corr", order="descending"),
    title=None
)

# Draw a lollipop chart of correlations
sticks = alt.Chart(corr_df_plot).mark_rule(strokeWidth=2).encode(
    y=y_axis,
    x=alt.X("zero:Q", scale=alt.Scale(domain=x_domain), title="Correlation with log(album listeners)"),
    x2="corr:Q",
    color=alt.condition(
        "datum.corr >= 0",
        alt.value("#1195B2"),
        alt.value("#CC0000")
    )
)

dots = alt.Chart(corr_df_plot).mark_circle(size=80).encode(
    y=y_axis,
    x=x_axis,
    color=alt.condition(
        "datum.corr >= 0",
        alt.value("#1195B2"),
        alt.value("#CC0000")
    ),
    tooltip=[
        alt.Tooltip("feature:N", title="Feature"),
        alt.Tooltip("corr:Q", title="Correlation", format=".3f")
    ]
)

zero_line = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(
    strokeDash=[4, 4], opacity=0.6
).encode(
    x=alt.X("x:Q", scale=alt.Scale(domain=x_domain))
)

chart = (sticks + dots + zero_line).properties(
    width=650,
    height=900,
    title={
        "text": "Lollipop Plot: Which features scale with album popularity?",
        "subtitle": "Pearson correlations with log(album listeners); no single feature dominates"
    }
)
# auto-detected possible Altair chart: chart
try:
    st.altair_chart(chart, use_container_width=True)
except Exception:
    st.write(chart)

st.markdown("""
Findings: No single feature stands out as a strong driver of soundtrack album popularity\\. The strongest correlations are modest and are led by broad measures of film visibility—such as vote counts, revenue, budget, and ratings—rather than soundtrack\\-specific attributes or awards\\. Genre indicators and nomination flags show weaker, mixed relationships, and many film\\-level features cluster close to zero\\. Even high\\-profile awards \\(including major nominations and wins\\) exhibit only small positive correlations\\. Taken together, the pattern suggests that album popularity reflects the combined effect of many small signals tied to overall film exposure, rather than any one dominant creative, genre, or awards\\-related factor\\.
""")

st.markdown("""
### II\\.4 Correlation sanity check
""")

st.markdown("""
Before moving on, we run a small set of correlation sanity checks to ensure that the relationships observed so far are not artifacts of a particular correlation metric or driven by a small number of extreme albums\\. These checks are not intended to uncover new patterns, but to confirm that the feature–popularity relationships are stable under reasonable alternative assumptions\\.
""")

st.markdown("""
Question:  Do we tell the same "top correlated features" story if we compute correlations
 using Pearson \\(scale\\-based\\) vs Spearman \\(rank\\-based\\)?
""")

# ------------------------------------------------------------
# Correlation sanity check (target-only):
# Do we get a similar "top features" story under Pearson vs Spearman?
#
# Why this matters:
# - Spearman (rank-based) is robust to heavy tails and nonlinearity.
# - Pearson (scale-based) reflects magnitude relationships (especially in log space).
# If the top features and overall rankings are similar under both, our conclusions
# are less sensitive to the choice of correlation metric.
# ------------------------------------------------------------

target = corr_df.columns[-1]
TOP_N = 15

# Keep only numeric columns and drop rows with missing values so comparisons are apples-to-apples
df = corr_df.dropna()

# Correlations with the target (one number per feature)
pearson = df.corr(method="pearson")[target].drop(target)
spearman = df.corr(method="spearman")[target].drop(target)

# Top-N overlap (by absolute correlation)
top_pearson = set(pearson.abs().nlargest(TOP_N).index)
top_spearman = set(spearman.abs().nlargest(TOP_N).index)
overlap = sorted(top_pearson & top_spearman)

# Overall agreement: Spearman correlation between the rank orders of |corr|
# This feels very meta, but run Spearman correlation on the top ranking features
# that came out of the original pearson and original spearman correlation
rank_agreement = (
    pearson.abs().rank(ascending=False)
    .corr(spearman.abs().rank(ascending=False), method="spearman")
)

# Display: one compact summary table + two headline stats
summary = pd.DataFrame({
    "pearson": pearson,
    "spearman": spearman,
})
summary["abs_pearson"] = summary["pearson"].abs()
summary["abs_spearman"] = summary["spearman"].abs()

print(f"Top-{TOP_N} overlap: {len(overlap)} / {TOP_N}")
print(f"Rank agreement (ρ on ranks of |corr|): {rank_agreement:.3f}")
print("Overlapping features:", overlap)

display(
    summary.sort_values("abs_pearson", ascending=False)
           .head(TOP_N)[["pearson", "spearman"]]
           .round(3)
)

st.markdown("""
Findings: The features most closely associated with album popularity are highly consistent under both Pearson and Spearman correlations\\. With the inclusion of film runtime, the top\\-15 features now align perfectly across the two methods \\(15/15 overlap\\), and the overall agreement in feature rankings remains very high \\(ρ ≈ 0\\.88\\)\\. This indicates that the results are not sensitive to whether correlation is measured on the original scale or by rank ordering\\.
""")

st.markdown("""
Question:
 Are our correlations with album popularity being driven by a tiny number of
 blockbuster albums?
""")

# ============================================================
# Test 2 — Trim extremes (outlier sensitivity)
# ============================================================

#
# Output:
# - stability of the correlation vector before vs after trimming (one number)
#   for Pearson and Spearman
# ------------------------------------------------------------

TRIM_Q = 0.99  # drop top 1% of albums by target

thr = df[target].quantile(TRIM_Q)
df_trim = df[df[target] <= thr]

pearson_trim = df_trim.corr(method="pearson")[target].drop(target)
spearman_trim = df_trim.corr(method="spearman")[target].drop(target)

# "Vector stability": correlation between the full and trimmed correlation vectors
pearson_stability = pearson.corr(pearson_trim, method="pearson")
spearman_stability = spearman.corr(spearman_trim, method="pearson")

print("\nTest 2 — Trim extremes")
print(f"Trim threshold (top {(1-TRIM_Q)*100:.1f}% removed): {thr:.3f}")
print(f"Rows kept: {len(df_trim):,} / {len(df):,}")
print(f"Pearson stability (full vs trimmed):  {pearson_stability:.3f}")
print(f"Spearman stability (full vs trimmed): {spearman_stability:.3f}")

st.markdown("""
Findings: Removing the most extreme albums by listener count has little effect on the overall correlation structure\\. Correlations before and after trimming remain highly aligned \\(Pearson = 0\\.96; Spearman = 0\\.99\\), indicating that the observed relationships are not driven by a small number of blockbuster soundtracks and are robust to the presence of outliers\\.
""")

st.markdown("""
# III\\. Hypothesis testing
""")

st.markdown("""
### III\\.1 Popular films \\-\\-\\> more listened albums
""")

st.markdown("""
Hypothesis: Albums tied to box office hits have higher listener counts\\.
""")

st.markdown("""
Film popularity can be measured in several closely related ways, including vote counts, budget, box office revenue, and platform\\-level popularity scores\\. These variables are known to be highly collinear and largely reflect the same underlying concept: overall film exposure\\. Rather than treating them as independent predictors, we test each as an alternative operationalization of film popularity to evaluate whether the same directional relationship holds across measures\\. This approach allows us to assess the robustness of the “popular film → popular soundtrack” relationship without over\\-interpreting any single metric\\.
""")

# Variables of interest
# Collinear features
collinear = ['film_vote_count', 'film_budget', 'film_popularity', 'film_revenue']

y = album_analytics_df["log_lfm_album_listeners"]

results = {}
for feature in collinear:
    x = album_analytics_df[feature]

    # Drop missing values pairwise
    mask = x.notna() & y.notna()
    x_valid = x[mask]
    y_valid = y[mask]

    # Spearman rank correlation test
    rho, p_value = spearmanr(x_valid, y_valid)

    alpha = 0.05
    if p_value < alpha:
        test = f"Result: Reject H₀ — evidence of a positive association with {feature}."
    else:
        test = f"Result: Fail to reject H₀ — no evidence of association with {feature}."

    results[feature] = (round(rho, 6), p_value, test)

st.dataframe(pd.DataFrame(results, index=["rho", "p_value", "test"]).T.round(2))

st.markdown("""
Findings: All four film popularity measures show a statistically significant positive association with album listener counts\\. The strength of the relationship varies by metric, with vote count exhibiting the strongest correlation, followed by budget and revenue, and popularity scores somewhat weaker\\. Given the high degree of collinearity among these features, these results should be interpreted as consistent evidence of a shared exposure effect rather than four independent findings\\. Taken together, they support the conclusion that soundtracks tied to more widely seen and discussed films tend to attract more listeners\\.
""")

st.markdown("""
### III\\.2 Prestige factor: do awards matter?
""")

st.markdown("""
Hypothesis: Award\\-recognized scores attract more listeners than non\\-recognized scores\\.
""")

y = album_analytics_df["log_lfm_album_listeners"]

award_features = ["us_score_nominee_count", "us_song_nominee_count", "bafta_nominee"]

results = {}
alpha = 0.05

for feature in award_features:
    x = album_analytics_df[feature]

    # Pairwise drop missing values
    mask = x.notna() & y.notna()
    x_valid = x[mask]
    y_valid = y[mask]

    # --- Welch setup: define "recognized" vs "not recognized" ---
    # Recognized = x > 0 (works for counts and 0/1 flags)
    y_recognized = y_valid[x_valid > 0]
    y_not        = y_valid[x_valid == 0]

    # Welch’s t-test (unequal variances)
    t_stat, p_value = ttest_ind(
        y_recognized,
        y_not,
        equal_var=False,
        nan_policy="omit"
    )

    # Direction (helpful for interpretation)
    mean_rec = y_recognized.mean()
    mean_not = y_not.mean()
    diff = mean_rec - mean_not  # positive means recognized > not

    if p_value < alpha and diff > 0:
        test = f"Reject H₀ — recognized group has higher mean listeners for {feature}."
    elif p_value < alpha and diff < 0:
        test = f"Reject H₀ — recognized group has LOWER mean listeners for {feature}."
    else:
        test = f"Fail to reject H₀ — no clear mean difference for {feature}."

    results[feature] = (len(y_recognized), len(y_not), mean_rec, mean_not, diff, t_stat, p_value, test)

display(
    pd.DataFrame(
        results,
        index=["n_rec", "n_not", "mean_rec", "mean_not", "mean_diff", "t_stat", "p_value", "test"]
    ).T
    .round(3)
)

st.markdown("""
Findings: Across all three award indicators, albums associated with award\\-recognized scores or songs have higher average listener counts than those without recognition\\. Using Welch’s t\\-tests to compare mean log listener counts between recognized and non\\-recognized groups, the differences are statistically significant for U\\.S\\. score nominations, U\\.S\\. song nominations, and BAFTA nominations\\. In each case, the award\\-recognized group shows meaningfully higher average listeners, supporting the hypothesis that industry recognition is associated with greater soundtrack popularity\\.
""")

st.markdown("""
### III\\.3 Genre effects
""")

st.markdown("""
Hypothesis: Listener interest differs by soundtrack musical style\\.
""")

st.markdown("""
Because we transformed NaNs into False flags for genre in order to build the earlier correlation table, we cannot use album\\_analytics\\_df for this next hypothesis test\\. We need to leverage the original albums\\_df \\(which still contain NaNs in the rg\\_tags\\_text column\\)\\.
""")

genre_features = [
    "ambient_experimental", "classical_orchestral", "electronic",
    "hip_hop_rnb", "pop", "rock", "world_folk"
]

y_col = "log_lfm_album_listeners"

# Only albums with genre tags present
df_genre = albums_df[
    albums_df["rg_tags_text"].notna() &
    albums_df[y_col].notna()
]

alpha = 0.05 / len(genre_features)
results = {}

for g in genre_features:
    # Explicit in-genre vs not-in-genre (both have tags)
    y_in  = df_genre.loc[df_genre[g] == 1, y_col]
    y_out = df_genre.loc[df_genre[g] == 0, y_col]

    t_stat, p_value = ttest_ind(
        y_in, y_out,
        equal_var=False,
        nan_policy="omit"
    )

    mean_in = y_in.mean()
    mean_out = y_out.mean()
    diff = mean_in - mean_out

    test = (
        f"Reject H₀ — mean differs for {g}."
        if p_value < alpha
        else f"Fail to reject H₀ — no clear mean difference for {g}."
    )

    results[g] = (
        len(y_in), len(y_out),
        mean_in, mean_out,
        diff, t_stat, p_value, test
    )

display(
    pd.DataFrame(
        results,
        index=[
            "n_in_genre", "n_not",
            "mean_in", "mean_not",
            "mean_diff",
            "t_stat", "p_value", "test"
        ]
    ).T.round(3)
)

st.markdown("""
Findings: After restricting the analysis to albums with available genre metadata, I tested whether listener interest differed by soundtrack musical style using Welch’s t\\-tests\\. Across all seven album genres, I fail to reject the null hypothesis\\. While some genres \\(notably pop and rock\\) show higher average listener counts, these differences are not statistically significant once sampling variability is taken into account\\. Overall, the results suggest that album\\-level musical style alone does not meaningfully differentiate listener popularity in this dataset\\. Any apparent differences in means are small and inconsistent, indicating that genre by itself is not a strong driver of soundtrack listening behavior\\.
""")

st.markdown("""
### III\\.4 Composer signal
""")

st.markdown("""
Hypothesis: Established composers have higher\\-listened albums, even controlling for film popularity\\.
""")

st.markdown("""
To address this question, I fit a simple linear regression predicting log soundtrack listeners using composer album count as a proxy for composer experience, while controlling for film vote count as a measure of film popularity\\. This allows me to test whether composer experience is associated with listener interest beyond the effect of overall film exposure\\.
""")

# Select only what we need
cols = [
    "log_lfm_album_listeners",
    "composer_album_count",
    "film_vote_count",
]

df_test = album_analytics_df[cols].dropna()

y = df_test["log_lfm_album_listeners"]
X = df_test[["composer_album_count", "film_vote_count"]]

# Add intercept
X = sm.add_constant(X)

# Fit model
res = sm.OLS(y, X).fit()

print(res.summary())

st.markdown("""
Findings: After controlling for film popularity, there is no clear evidence that albums by more established composers attract more listeners\\. In this model, film vote count shows a strong positive association with soundtrack listeners, which is expected since it reflects overall film exposure\\. In contrast, composer album count has a small negative coefficient and a p\\-value just above the conventional 0\\.05 threshold\\. I interpret this as weak and inconclusive evidence that composer experience, as defined here, does not meaningfully increase listener interest once film popularity is taken into account\\.
""")

st.markdown("""
# IV Regression
""")

st.markdown("""
### IV\\.1 Winnow features
""")

st.markdown("""
Before fitting any regression models, we first narrow the feature set to something interpretable and stable\\. The goal here is not aggressive feature selection or optimization, but basic hygiene: grouping predictors by type, reducing obvious noise, and avoiding unnecessary complexity in the first\\-pass OLS\\. We bucket features into continuous and binary predictors, retain all binary indicators \\(genres, awards, flags\\), and apply a light correlation\\-based filter to continuous variables only\\. This keeps the regression focused on predictors with at least a minimal linear relationship to log album listeners, while preserving categorical context and avoiding premature assumptions about causal importance\\.
""")

# ------------------------------------------------------------
# IV) Regression prep
# 1) Feature groups (bucket into continuous and binary)
# 2) Light winnowing:
#    - Keep all binary flags (film genres, album genres, awards)
#    - Filter continuous predictors by |Pearson r| >= THRESH vs target
# Light winnowing is intentionally conservative:
# - We only filter continuous predictors
# - Binary flags are retained to preserve categorical context
# - Threshold is low by design; this is not feature selection, just noise reduction
# ------------------------------------------------------------

THRESH = 0.05
target = y_feature[0]  # 'log_lfm_album_listeners'

# -----------------------------
# Step 1) Define feature groups (from existing lists)
# Continuous variables are potential scale drivers in OLS
# Binary variables act as controls / group indicators and are not filtered
# -----------------------------
# Film-side continuous variables
film_continuous = [
    "film_vote_count", "film_popularity", "film_budget", "film_revenue",
    "film_rating", "days_since_film_release"
]

# Film-side binary variables all start with "film_is"
film_binary = [c for c in film_features if c.startswith("film_is_")]

# Album-side continuous and binary variables
album_continuous = ["days_since_album_release", "n_tracks", "composer_album_count"]
album_binary = [c for c in album_features if c not in album_continuous]

# Awards are binary flags (0/1)
awards_binary = derived_award_cols

continuous_features = film_continuous + album_continuous
binary_features = film_binary + album_binary + awards_binary

# -----------------------------
# Step 2) Light winnowing (continuous only)
# -----------------------------
df = album_analytics_df[binary_features + continuous_features + [target]].copy()

# Pearson correlations for continuous features vs target (pairwise complete)
cont_corr = df[[target] + continuous_features].corr(method="pearson")[target].drop(target)

kept_continuous = cont_corr[cont_corr.abs() >= THRESH].index.tolist()
dropped_continuous = cont_corr[cont_corr.abs() < THRESH].index.tolist()

# Final X list for OLS (binary all kept + filtered continuous)
# Final predictor list:
# - All binary flags
# - Only continuous predictors with a minimal linear signal vs target
X_cols = binary_features + kept_continuous

print(f"Continuous kept (|r| >= {THRESH:.2f}): {len(kept_continuous)} / {len(continuous_features)}")
print(f"Binary kept (no filter): {len(binary_features)}")
print(f"Final X feature count: {len(X_cols)}")

print("\nKept continuous features (sorted by |corr|):")
st.write(cont_corr.loc[kept_continuous].sort_values(key=lambda s: s.abs(), ascending=False).to_frame("pearson_r").round(3))

print("\nDropped continuous features:")
print(dropped_continuous)

st.markdown("""
### IV\\.2 Transform features
""")

st.markdown("""
Before fitting the regression, we pause to validate a core assumption of OLS: that continuous predictors relate to the outcome in an approximately linear, well\\-behaved way\\. Several of our film\\-side variables \\(votes, budget, revenue, popularity\\) are known to be heavy\\-tailed, and earlier correlation work already hinted that a small number of blockbuster films exert outsized influence\\. Rather than transforming by default, we run a compact diagnostic: a faceted scatterplot of log\\(album listeners\\) against log1p\\(feature\\) for each continuous predictor\\. This lets us quickly assess, in one view, whether log compression meaningfully improves linearity and variance stability, and whether any variables are better left on their original scale\\. The goal here is pragmatic: apply only the transformations that materially improve model behavior, and leave the rest untouched\\.
""")

# This is my evaluation of each pair-chart. Placing it here so it can be printed right above its
# corresponding pairwise charts
eval_map = {
    "film_vote_count": "Heavy-tailed; log1p should reduce leverage from blockbusters and make the slope easier to read.",
    "film_budget": "Heavy-tailed; raw scale is dominated by a few extremes — log1p should stabilize the trend.",
    "film_revenue": "Extremely heavy-tailed; log1p should compress the top tail and reveal mid-range structure.",
    "film_popularity": "Skewed; log1p often spreads the low-end pile-up and clarifies the relationship.",
    "film_rating": "Bounded; log1p usually won’t change much — expect similar shape either way.",
    "days_since_film_release": "Timing variable; already smooth/linear-ish — log1p is typically unnecessary.",
    "days_since_album_release": "Timing variable; raw should already be interpretable — log1p rarely adds value.",
    "n_tracks": "Discrete/bounded; log1p won’t materially change the relationship (look for a weak/flat slope).",
    "composer_album_count": "Discrete; expect limited effect size \n log1p mainly just re-scales the x-axis.",
}

# ------------------------------------------------------------
# Step 3 Diagnostic (revised):
# Raw vs log1p(feature), with a single evaluation line BETWEEN the pair.
#
# Key idea:
# - We first build a "long" dataframe that contains one row per observation
#   per (feature, scale).
# - IMPORTANT: we filter OUT x==0 for each feature when building "long"
#   so vertical stacks at 0 don't appear in the charts.
# - Then we build the visualization as a vertical stack (vconcat):
#     [evaluation text line]
#     [two-column chart: raw vs log1p]
#   repeated for each feature.
# ------------------------------------------------------------

y = y_feature[0]  # e.g., 'log_lfm_album_listeners'

# ------------------------------------------------------------
# 1) Start from the analytics dataframe and keep only needed columns.
#    We only drop rows where y is missing (because the scatter y-axis needs it).
#    NOTE: This does NOT remove x==0; we handle that per-feature below.
# ------------------------------------------------------------
diag_df = album_analytics_df[continuous_features + [y]].dropna(subset=[y]).copy()

# ------------------------------------------------------------
# 2) Build a LONG dataframe with BOTH:
#    - raw x values
#    - log1p(x) values
#
#    We do this per feature so we can apply per-feature filtering rules:
#    - drop rows where the feature is missing
#    - drop rows where the feature is exactly 0 (your request)
#
#    Resulting schema of `long`:
#      [ y, x, feature, scale ]
# ------------------------------------------------------------
rows = []

for c in continuous_features:

    # --- Base subset for this feature only ---
    # Keep rows where:
    # - the feature value exists (not NaN)
    # - the feature value is NOT 0 (so we remove those vertical stacks)
    #
    # If you ever decide you want "drop <= 0" instead, change (d[c] != 0) to (d[c] > 0).
    base = (
        diag_df[[y, c]]
        .dropna()                       # drop rows where this feature is missing
        .loc[lambda d: d[c] != 0]       # drop rows where this feature equals 0
        .copy()
    )

    # --- Raw version: x = feature value as-is ---
    raw = (
        base.rename(columns={c: "x"})   # rename feature column to generic "x"
            .assign(feature=c, scale="raw")
    )

    # --- Log1p version: x = log(1 + feature) ---
    # log1p is stable for large values and avoids log(0) issues.
    # We keep .clip(lower=0) just in case negatives exist (should be rare here).
    log = (
        base.assign(x=lambda d: np.log1p(d[c].clip(lower=0)))
            .loc[:, [y, "x"]]           # keep only y and the new x
            .assign(feature=c, scale="log1p")
    )

    rows.append(raw)
    rows.append(log)

# Combine all features/scales into one long dataframe
long = pd.concat(rows, ignore_index=True)

# Make sure the facet columns appear in the intended order: raw then log1p
long["scale"] = pd.Categorical(long["scale"], categories=["raw", "log1p"], ordered=True)

# ------------------------------------------------------------
# 3) Build the visualization as a set of "blocks" (one block per feature):
#    Block layout:
#      (a) evaluation text row
#      (b) a paired chart: raw vs log1p (two columns)
#
#    Then we vertically stack (vconcat) all blocks.
# ------------------------------------------------------------

blocks = []

for c in continuous_features:
    # Get the human-readable evaluation note for this feature
    note = eval_map.get(c, "")

    # --- (a) Evaluation text row ---
    # This is a tiny one-row dataframe, so it renders ONCE (not duplicated).
    eval_row = (
        alt.Chart(pd.DataFrame({"text": [f"{c}: {note}"]}))
        .mark_text(
            align="left",
            baseline="middle",
            fontSize=12,
            dx=6
        )
        .encode(text="text:N")
        .properties(height=24)
    )

    # --- (b) Paired chart for this feature only ---
    # We subset `long` to the current feature, which already has x==0 removed.
    sub = long[long["feature"] == c]

    # Scatter points
    points = (
        alt.Chart(sub)
        .mark_circle(opacity=0.20, size=28)
        .encode(
            x=alt.X("x:Q", title=None),
            y=alt.Y(f"{y}:Q", title="log(album listeners)"),
        )
    )

    # Trendline: regression within each scale (raw vs log1p)
    trend = (
        alt.Chart(sub)
        .transform_regression("x", y, groupby=["scale"])
        .mark_line(strokeWidth=3, opacity=0.9)
        .encode(
            x="x:Q",
            y=f"{y}:Q"
        )
    )

    # Facet into two columns: raw (left) and log1p (right)
    pair = (points + trend).facet(
        column=alt.Column("scale:N", title=None, sort=["raw", "log1p"])
    ).resolve_scale(
        x="independent"   # raw and log1p have different x ranges, so don't force them to match
    )

    # Stack evaluation row ABOVE the paired chart for this feature
    blocks.append(alt.vconcat(eval_row, pair, spacing=6))

# Stack all feature-blocks vertically
chart = alt.vconcat(*blocks, spacing=14).resolve_scale(
    y="shared"  # keep y-axis consistent across all features for easier comparison
).properties(
    title={
        "text": "Raw vs log1p diagnostics for continuous predictors",
        "subtitle": [
            "Each feature is shown as a paired view (raw vs log1p).",
            "Rows where the feature value equals 0 are excluded from that feature’s plots."
        ]
    }
)

# auto-detected possible Altair chart: chart
try:
    st.altair_chart(chart, use_container_width=True)
except Exception:
    st.write(chart)

st.markdown("""
Findings: Looking across the nine diagnostic plots, a few patterns stood out pretty clearly\\. Several film\\-level exposure variables—especially revenue, vote count, and budget \\(and to a lesser extent popularity\\)—are very right\\-skewed\\. In the raw plots, the regression lines are strongly influenced by a small number of blockbuster films, while most albums are crowded into a much narrower range\\. On the raw scale, this makes it hard to see how these variables relate to album listeners for the majority of observations\\.
""")

st.markdown("""
When these same variables are plotted using a log \\(or log1p\\) transform, the extreme values are pulled in and the overall patterns are easier to read\\. The relationships don’t suddenly become strong or perfectly linear, but the fitted lines appear less dominated by outliers and more reflective of the bulk of the data\\. Based on these diagnostics, using a log transform for these heavily skewed exposure variables seems reasonable before fitting linear models\\.
""")

st.markdown("""
In contrast, the timing variables \\(days since film release and days since album release\\) already show fairly smooth, straight\\-forward relationships with log\\(album listeners\\) on the raw scale, and the log transform does not appear to change their behavior in a meaningful way\\. Similarly, discrete and bounded features such as track count, composer album count, and film rating show weak but consistent patterns across both scales\\. In these cases, the small slopes appear to reflect genuinely limited effects rather than problems with scaling\\.
""")

st.markdown("""
Overall, these diagnostics suggest that log\\-transforming the most skewed exposure variables is helpful, while leaving timing and bounded variables on their original scales is likely sufficient for the regression models used later in the analysis\\.
""")


# -----------------------------
# Step 3 — Apply transforms
# -----------------------------
df_model = album_analytics_df[X_cols + [target]].copy()

# Identify predictor types
X_cont = [c for c in X_cols if c not in binary_features]  # continuous numeric predictors
X_bin  = [c for c in X_cols if c in binary_features]      # 0/1 flags (genres, awards, etc.)

# (a) Log-transform ONLY the heavy-tailed exposure variables (if present)
# SPLOM justification: these are strongly right-skewed and benefit from log compression.
HEAVY_TAILED = ["film_vote_count", "film_budget", "film_revenue", "film_popularity"]

for c in HEAVY_TAILED:
    if c in X_cont:
        # log1p handles zeros; clip avoids issues if anything weird slipped in
        df_model[c] = np.log1p(df_model[c].clip(lower=0))

# (b) Z-score all continuous predictors (post-log where applicable)
# Recommended because it puts continuous predictors on a common scale and makes
# coefficients easier to compare (does NOT change model fit).
for c in X_cont:
    mu = df_model[c].mean(skipna=True)
    sd = df_model[c].std(skipna=True, ddof=0)

    # Avoid divide-by-zero if a column is constant
    if sd == 0 or np.isnan(sd):
        df_model[c] = df_model[c] - mu
    else:
        df_model[c] = (df_model[c] - mu) / sd

# Binary predictors stay as 0/1 (no scaling, no transforms)
# Target is already log-transformed upstream in your pipeline.

print("Step 3 complete.")
print(f"Modeling frame shape: {df_model.shape}")
print(f"Continuous predictors standardized: {len(X_cont)}")
print(f"Binary predictors untouched: {len(X_bin)}")
print("Logged predictors (if present):", [c for c in HEAVY_TAILED if c in X_cont])

# df_model is now ready for Step 4 (OLS)

st.markdown("""
Based on the diagnostic plots, I log\\-transformed only the film exposure variables that showed strong right skew \\(vote count, budget, and revenue\\)\\. All continuous predictors were then standardized so they are on a common scale, while binary indicators were left unchanged\\. The target variable was already log\\-transformed earlier\\. This produces a clean modeling dataset that is ready for OLS regression\\.
""")

st.markdown("""
### IV\\.3 Simple linear regression
""")

st.markdown("""
Let's do a final check and cleanup of our df\\_model before running regression\\. We should drop addition features that we've seen to be highly correlated to reduce multicollinearity before running regression
""")

print(df_model.columns)

# To prevent multicollinearity:
# 1) Drop days_since_film_release (nearly perfectly correlated with days_since_album_release)
# 2) Keep only ONE film exposure proxy (film_vote_count) and drop the rest
df_model_final = df_model.drop(columns=[
    "days_since_film_release",
    "film_budget",
    "film_revenue"
])

# The film_is columns are boolean -- convert them to int
film_genre_cols = [c for c in df_model_final.columns if c.startswith("film_is_")]

df_model_final[film_genre_cols] = df_model_final[film_genre_cols].astype(int)

print("Final model:", df_model_final.columns)

st.markdown("""
At this stage, df\\_model already contains only the finalized predictors, so the regression uses all remaining columns except the target variable\\.
""")

# ------------------------------------------------------------
# Final OLS regression (minimal + correct ordering)
# ------------------------------------------------------------

y_col = "log_lfm_album_listeners"

# Use all remaining columns except the target
X_cols = [c for c in df_model_final.columns if c != y_col]

# ------------------------------------------------------------
# 1) Build modeling frame and force numeric types
# ------------------------------------------------------------

model_df_reg = df_model_final[[y_col] + X_cols].copy()

# Coerce everything to numeric (required by statsmodels)
model_df_reg = model_df_reg.apply(pd.to_numeric, errors="coerce")

# Drop rows with any missing values
model_df_reg = model_df_reg.dropna()

print("Modeling rows:", model_df_reg.shape[0])
print("Predictors:", len(X_cols))

# ------------------------------------------------------------
# 2) Split into X and y
# ------------------------------------------------------------

y = model_df_reg[y_col]
X = model_df_reg[X_cols]

# Add intercept
X = sm.add_constant(X)

# ------------------------------------------------------------
# 3) Fit OLS model
# ------------------------------------------------------------

results = sm.OLS(y, X).fit()

print(results.summary())

st.markdown("""
Findings\\. Using a simplified OLS model with 744 soundtrack albums and 32 predictors, I find that overall film exposure is the strongest and most consistent driver of soundtrack popularity\\. Film vote count shows a large, positive, and highly significant association with log album listeners, even after controlling for film genres, album genres, timing, and awards\\. This reinforces the idea that soundtrack listening is closely tied to how widely seen or discussed the underlying film is, rather than to many of the finer\\-grained film or album characteristics\\.
""")

st.markdown("""
Several content\\-related signals emerge in intuitive ways\\. Animated films and documentaries are associated with substantially higher soundtrack listening, and music\\-focused films also show a positive effect that is close to conventional significance thresholds\\. On the album side, classical/orchestral soundtracks are positively associated with listener counts, while electronic soundtracks show a similar but slightly weaker pattern\\. These effects suggest that certain film types and musical styles are more likely to generate sustained soundtrack engagement\\.
""")

st.markdown("""
In contrast, most genre indicators—both film and album—do not show strong independent effects once exposure is accounted for\\. This appears to reflect the fact that genre primarily shapes what kind of soundtrack a listener encounters, rather than how many listeners it ultimately attracts\\. Similarly, award nominations \\(both U\\.S\\. and BAFTA\\) do not show a meaningful relationship with soundtrack listening in this model, suggesting that critical recognition does not translate directly into broader listener demand once exposure is controlled\\.
""")

st.markdown("""
Finally, timing effects are modest and statistically insignificant, indicating that once a soundtrack has been released, simple “age since release” does not strongly explain differences in cumulative listener counts\\. Overall, the model explains a meaningful but limited share of variation in soundtrack popularity \\(R² ≈ 0\\.14\\), which is reasonable given the many unobserved factors that influence music consumption\\. Taken together, these results support a simple conclusion: soundtrack popularity is driven primarily by film reach, with secondary contributions from certain film categories and musical styles, rather than by awards or fine\\-grained genre distinctions\\.
""")

st.markdown("""
### IV\\.4 Regression Visualizations
""")

st.markdown("""
Our strongest variable is film\\_vote\\_count \\-\\- so let's generate a scatterplot of it against log album listeners
""")

# Use the exact regression sample
plot_df = model_df_reg[["film_vote_count", "log_lfm_album_listeners"]].copy()

# Fit simple line in Python: y = a*x + b
x = plot_df["film_vote_count"].to_numpy()
y = plot_df["log_lfm_album_listeners"].to_numpy()
a, b = np.polyfit(x, y, 1)   # Fit the best-fitting line across the datapoints

# Make a 2-point line spanning the x-range
x_min, x_max = float(x.min()), float(x.max())
line_df = pd.DataFrame({
    "film_vote_count": [x_min, x_max],
    "log_lfm_album_listeners": [a * x_min + b, a * x_max + b]
})

scatter = alt.Chart(plot_df).mark_circle(opacity=0.25, size=40).encode(
    x=alt.X("film_vote_count:Q", title="Film vote count (exposure proxy)"),
    y=alt.Y("log_lfm_album_listeners:Q", title="Log soundtrack listeners")
)

line = alt.Chart(line_df).mark_line(strokeWidth=3).encode(
    x="film_vote_count:Q",
    y="log_lfm_album_listeners:Q"
)

(scatter + line).properties(
    width=650,
    height=400,
    title="Film exposure vs soundtrack popularity (with fitted line)"
)

st.markdown("""
Findings: This figure shows the relationship between film exposure and soundtrack popularity for the albums used in the regression\\. Each point is a soundtrack album, with film exposure measured by film vote count and popularity measured as log soundtrack listeners\\. Film vote count was log\\-transformed earlier in the analysis, which is why the x\\-axis values look compressed and include negative numbers\\.
""")

st.markdown("""
Despite substantial scatter, there is a clear positive trend: soundtracks tied to more widely voted\\-on films tend to attract more listeners on average\\. The fitted line reflects this pattern and mirrors the regression result, where film vote count emerged as the strongest and most consistent predictor of soundtrack popularity\\.
""")

# -----------------------------
# 1) Build tidy coefficient table
# -----------------------------
coef_df = (
    pd.DataFrame({
        "feature": results.params.index,
        "coef": results.params.values,
        "ci_low": results.conf_int()[0].values,
        "ci_high": results.conf_int()[1].values,
    })
    .query("feature != 'const'")
    .copy()
)

# CI crosses zero? (True/False)
coef_df["crosses_zero"] = (coef_df["ci_low"] <= 0) & (coef_df["ci_high"] >= 0)

# Make it a *string category* so Altair treats it as two discrete groups reliably
coef_df["ci_group"] = coef_df["crosses_zero"].map(
    {True: "CI crosses 0", False: "CI does NOT cross 0"}
)

# Sort so the largest absolute effects appear first (top)
coef_df["abs_coef"] = coef_df["coef"].abs()
coef_df = coef_df.sort_values("abs_coef", ascending=False)

# Lock the y-order to this sorted order
y_order = coef_df["feature"].tolist()

# -----------------------------
# 2) Dot-and-whisker plot (theme colors)
# -----------------------------

whiskers = alt.Chart(coef_df).mark_rule().encode(
    x="ci_low:Q",
    x2="ci_high:Q",
    y=alt.Y("feature:N", sort=y_order, title=None),
    color=alt.Color("ci_group:N", title=None)  # uses theme category palette
)

dots = alt.Chart(coef_df).mark_circle(size=80).encode(
    x=alt.X("coef:Q", title="Effect on log soundtrack listeners"),
    y=alt.Y("feature:N", sort=y_order, title=None),
    color=alt.Color("ci_group:N", title=None, legend=None),
    tooltip=[
        alt.Tooltip("feature:N", title="Feature"),
        alt.Tooltip("coef:Q", format=".3f", title="Coefficient"),
        alt.Tooltip("ci_low:Q", format=".3f", title="CI low"),
        alt.Tooltip("ci_high:Q", format=".3f", title="CI high"),
        alt.Tooltip("ci_group:N", title="")
    ]
)

zero = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(strokeDash=[4, 4]).encode(
    x="x:Q"
)

chart = (whiskers + dots + zero).properties(
    width=750,
    height=700,
    title={
        "text": "Regression coefficients with 95% confidence intervals",
        "subtitle": [
            "Dots are coefficient estimates; whiskers are 95% confidence intervals.",
            "Color indicates whether the confidence interval crosses 0."
        ]
    }
)

# auto-detected possible Altair chart: chart
try:
    st.altair_chart(chart, use_container_width=True)
except Exception:
    st.write(chart)

st.markdown("""
Findings: This chart shows the regression coefficients with their 95% confidence intervals\\. Each dot is the estimated effect of a feature on log soundtrack listeners, and the horizontal lines show how uncertain that estimate is\\. The dashed vertical line marks zero \\(no effect\\)\\.
""")

st.markdown("""
A small number of variables clearly stand out because their confidence intervals do not cross zero — most notably film vote count \\(our exposure proxy\\), documentary films, animation, and a few music\\-related genres\\. These are the features where the model suggests a more reliable association with soundtrack popularity\\.
""")

st.markdown("""
Most other features have confidence intervals that cross zero, which means their estimated effects are noisy and could plausibly be positive or negative\\. I interpret those as weak or inconclusive signals rather than meaningful drivers\\. Overall, this reinforces the idea that film exposure matters most, while genre and award features tend to have smaller and less stable effects\\.
""")

# We should export the album_analytics_df (before the extreme feature pruning we did) for the final visualization
st.dataframe(album_analytics_df.head())

album_analytics_df.to_csv("./pipeline/5.5.Albums_for_final_viz.csv", index = False)
