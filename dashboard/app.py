"""
OtakuLens — Your lens into the anime world.
Premium Streamlit dashboard for the MAL anime data pipeline.
"""

import os
import re
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import duckdb
from pathlib import Path
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OtakuLens",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700;800&display=swap');

/* Global overrides */
html, body, p, h1, h2, h3, h4, h5, h6, .stMarkdown, .stText, .stSelectbox label {
    font-family: 'Poppins', sans-serif !important;
}

/* Hide default header / footer */
header[data-testid="stHeader"] { background: transparent; }
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

/* ─── Header ─── */
.header-container {
    text-align: center;
    padding: 1.5rem 0 0.5rem;
}
.header-title {
    font-size: 2.6rem;
    font-weight: 800;
    background: linear-gradient(135deg, #e4a853, #f7d794, #e4a853);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: 2px;
    margin-bottom: 0;
}
.header-tagline {
    font-size: 0.95rem;
    color: #888;
    font-weight: 300;
    letter-spacing: 1px;
    margin-top: 0;
}
.gold-divider {
    height: 2px;
    background: linear-gradient(90deg, transparent, #e4a853, transparent);
    border: none;
    margin: 0.6rem auto 1.8rem;
    width: 60%;
}

/* ─── Scorecard ─── */
.card {
    background: #1f1a15;
    border-top: 3px solid #e4a853;
    border-radius: 12px;
    padding: 1.1rem 0.8rem;
    text-align: center;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.card:hover {
    transform: translateY(-3px);
    box-shadow: 0 6px 20px rgba(228, 168, 83, 0.15);
}
.card-icon {
    font-size: 1.3rem;
    margin-bottom: 0.25rem;
}
.card-value {
    font-size: 1.55rem;
    font-weight: 700;
    color: #f0f0f0;
    margin: 0.15rem 0;
    min-height: 2.4rem;
    display: flex;
    align-items: center;
    justify-content: center;
}
.card-value-small {
    font-size: 0.85rem;
    font-weight: 600;
    color: #f0f0f0;
    margin: 0.15rem 0;
    line-height: 1.3;
    min-height: 2.4rem;
    display: flex;
    align-items: center;
    justify-content: center;
}
.card-label {
    font-size: 0.72rem;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    font-weight: 500;
}

/* ─── Welcome state ─── */
.welcome-container {
    text-align: center;
    padding: 4rem 2rem;
    color: #888;
}
.welcome-icon {
    font-size: 4rem;
    margin-bottom: 1rem;
}
.welcome-text {
    font-size: 1.2rem;
    font-weight: 300;
    color: #aaa;
}

/* ─── Genre / theme chips ─── */
.chip-row { display: flex; flex-wrap: wrap; gap: 0.45rem; margin: 0.6rem 0; }
.chip {
    display: inline-block;
    border: 1px solid #e4a853;
    color: #e4a853;
    border-radius: 999px;
    padding: 0.2rem 0.75rem;
    font-size: 0.72rem;
    font-weight: 500;
    letter-spacing: 0.5px;
    transition: background 0.2s;
}
.chip:hover { background: rgba(228, 168, 83, 0.12); }
.chip.theme-chip {
    border-color: #7c6f9f;
    color: #b8a9d4;
}
.chip.theme-chip:hover { background: rgba(124, 111, 159, 0.12); }

/* ─── Hero section ─── */
.hero-title {
    font-size: 1.8rem;
    font-weight: 700;
    color: #f0f0f0;
    margin-bottom: 0;
    line-height: 1.2;
}
.jp-title {
    font-size: 1rem;
    color: #888;
    font-weight: 300;
    font-style: italic;
    margin-top: 0.15rem;
    margin-bottom: 0.7rem;
}
.meta-badges {
    display: flex;
    flex-wrap: wrap;
    gap: 0.85rem;
    margin: 0.6rem 0;
    font-size: 0.82rem;
    color: #bbb;
}
.meta-badges span { white-space: nowrap; }
.synopsis-text {
    font-size: 0.88rem;
    color: #ccc;
    line-height: 1.65;
    margin-top: 0.8rem;
}

/* ─── Character grid ─── */
.char-card {
    text-align: center;
    transition: transform 0.2s ease;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 0.3rem;
    margin-bottom: 1.5rem;
}
.char-card:hover { transform: scale(1.04); }
.char-card img {
    border-radius: 10px;
    border: 2px solid #1a1a2e;
    transition: border-color 0.2s ease;
}
.char-card:hover img { border-color: #e4a853; }
.char-name {
    font-size: 0.75rem;
    color: #ccc;
    margin-top: 0.3rem;
    font-weight: 400;
}

/* ─── Section headers ─── */
.section-header {
    font-size: 1.15rem;
    font-weight: 600;
    color: #e4a853;
    margin: 2rem 0 0.8rem;
    letter-spacing: 0.5px;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.section-header::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(90deg, rgba(228,168,83,0.4), transparent);
}

/* ─── Plotly chart containers ─── */
.chart-container {
    background: #12121a;
    border-radius: 12px;
    padding: 0.8rem;
    border: 1px solid #1a1a2e;
}

/* ─── Data source badge ─── */
.backend-badge {
    position: fixed;
    bottom: 12px;
    right: 16px;
    background: #12121a;
    border: 1px solid #2a2a3a;
    border-radius: 8px;
    padding: 0.3rem 0.7rem;
    font-size: 0.65rem;
    color: #666;
    z-index: 999;
    letter-spacing: 0.5px;
}

/* ─── Marquee Banner ─── */
.marquee-container {
    width: 100vw;
    overflow: hidden;
    white-space: nowrap;
    box-sizing: border-box;
    margin-left: -50vw;
    margin-top: -3rem;
    margin-bottom: 2rem;
    left: 50%;
    position: relative;
    background: #0a0a0f;
    padding: 10px 0;
    border-bottom: 1px solid #1a1a2e;
}
.marquee-track {
    display: inline-block;
    animation: marquee 35s linear infinite;
}
@keyframes marquee {
    0% { transform: translateX(0); }
    100% { transform: translateX(-50%); }
}
.marquee-item {
    display: inline-block;
    height: 140px;
    margin: 0 8px;
    border-radius: 6px;
    box-shadow: 0 4px 10px rgba(0,0,0,0.5);
    border: 1px solid #1a1a2e;
}
</style>
""", unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_number(n) -> str:
    """1900000 → '1.9M', 45000 → '45K'."""
    if n is None or pd.isna(n):
        return "—"
    n = int(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(n)


def fmt_date(ts) -> str:
    """Timestamp → 'Apr 2013'."""
    if ts is None or pd.isna(ts):
        return "—"
    return pd.Timestamp(ts).strftime("%b %Y")


# ── Backend connection ────────────────────────────────────────────────────────

@st.cache_resource
def get_backend():
    """Return (connection, backend_type).  Tries BigQuery first, falls back to DuckDB."""
    try:
        from google.cloud import bigquery
        project = GCP_PROJECT_ID or st.secrets.get("GCP_PROJECT_ID", "")
        if "gcp_service_account" in st.secrets:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_info(
                dict(st.secrets["gcp_service_account"])
            )
            client = bigquery.Client(project=project, credentials=creds)
        else:
            client = bigquery.Client(project=project)
        client.get_dataset("mal_pipeline")
        return client, "bq"
    except Exception:
        db_path = Path(__file__).resolve().parent.parent / "data" / "mal.duckdb"
        con = duckdb.connect(str(db_path), read_only=True)
        return con, "duckdb"


def run_query(bq_sql: str, duck_sql: str) -> pd.DataFrame:
    """Execute SQL against whichever backend is active."""
    conn, backend = get_backend()
    if backend == "bq":
        return conn.query(bq_sql).to_dataframe()
    return conn.execute(duck_sql).fetchdf()


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_resource
def load_embedding_model():
    return SentenceTransformer("all-MiniLM-L6-v2")

@st.cache_data(ttl=3600)
def load_all_anime_for_similarity():
    return run_query(
        f"SELECT anime_id, title, title_english, synopsis, genre_1, genre_2, genre_3, theme_1, theme_2, studios, image_url, score, anime_type FROM `{GCP_PROJECT_ID}.mal_pipeline.mart_anime`",
        "SELECT anime_id, title, title_english, synopsis, genre_1, genre_2, genre_3, theme_1, theme_2, studios, image_url, score, anime_type FROM mart_anime"
    )

@st.cache_data
def compute_embeddings(_model, df):
    def build_text(r):
        synopsis = str(r.get("synopsis")) if pd.notna(r.get("synopsis")) else ""
        genres = " ".join(str(r.get(c)) for c in ["genre_1", "genre_2", "genre_3"] if pd.notna(r.get(c)))
        themes = " ".join(str(r.get(c)) for c in ["theme_1", "theme_2"] if pd.notna(r.get(c)))
        studios = str(r.get("studios")) if pd.notna(r.get("studios")) else ""
        # repeat genres/themes 3x to outweigh long synopsis text
        return f"{synopsis} {genres} {genres} {genres} {themes} {themes} {themes} {studios}".strip()

    texts = df.apply(build_text, axis=1).tolist()
    return _model.encode(texts, show_progress_bar=False)


def get_base_title(title: str) -> str:
    s = str(title).lower()
    s = re.sub(r'\b\d+(st|nd|rd|th)\b', '', s)  # remove 2nd, 3rd, etc.
    s = re.sub(r'\b(season|part|chapter|the|movie)\b', '', s)
    s = re.split(r"[:\-–]", s)[0]
    s = re.sub(r'[^a-z0-9\s]+$', '', s.strip())  # remove trailing punctuation like ' or .
    s = re.sub(r'\s+\d+$', '', s)  # remove trailing numbers like 'gintama 2' -> 'gintama'
    return s.strip()


def get_similar_anime(anime_id, df, embeddings, n=6):
    idx = df.index[df["anime_id"] == anime_id].tolist()
    if not idx:
        return pd.DataFrame()

    selected = df.iloc[idx[0]]
    base = get_base_title(selected.get("title") or "")
    selected_type = selected.get("anime_type")

    scores = cosine_similarity([embeddings[idx[0]]], embeddings)[0]
    ranked = np.argsort(scores)[::-1][1:]  # skip self

    results = []
    for i in ranked:
        row = df.iloc[i]
        if get_base_title(row.get("title") or "") == base:
            continue
        if selected_type and pd.notna(selected_type) and row.get("anime_type") != selected_type:
            continue
        results.append(i)
        if len(results) == n:
            break

    return df.iloc[results]

@st.cache_data(ttl=3600)
def load_anime_list() -> pd.DataFrame:
    query_bq = f"SELECT anime_id, title, title_english, popularity_rank, genre_1, genre_2, genre_3, theme_1, theme_2, studios, anime_type FROM `{GCP_PROJECT_ID}.mal_pipeline.mart_anime` ORDER BY popularity_rank"
    query_duck = "SELECT anime_id, title, title_english, popularity_rank, genre_1, genre_2, genre_3, theme_1, theme_2, studios, anime_type FROM mart_anime ORDER BY popularity_rank"
    return run_query(query_bq, query_duck)


@st.cache_data(ttl=3600)
def load_top_posters() -> list:
    df = run_query(
        f"SELECT image_url FROM `{GCP_PROJECT_ID}.mal_pipeline.mart_anime` ORDER BY popularity_rank LIMIT 20",
        "SELECT image_url FROM mart_anime ORDER BY popularity_rank LIMIT 20"
    )
    return df["image_url"].dropna().tolist()


@st.cache_data(ttl=3600)
def load_anime_detail(anime_id: int) -> pd.Series:
    df = run_query(
        f"SELECT * FROM `{GCP_PROJECT_ID}.mal_pipeline.mart_anime` WHERE anime_id = {anime_id}",
        f"SELECT * FROM mart_anime WHERE anime_id = {anime_id}",
    )
    return df.iloc[0] if len(df) > 0 else pd.Series()


@st.cache_data(ttl=3600)
def load_characters(anime_id: int) -> pd.DataFrame:
    return run_query(
        f"SELECT * FROM `{GCP_PROJECT_ID}.mal_pipeline.mart_characters` WHERE anime_id = {anime_id}",
        f"SELECT * FROM mart_characters WHERE anime_id = {anime_id}",
    )


@st.cache_data(ttl=3600)
def load_episodes(anime_id: int) -> pd.DataFrame:
    return run_query(
        f"SELECT * FROM `{GCP_PROJECT_ID}.mal_pipeline.mart_episodes` WHERE anime_id = {anime_id} ORDER BY episode_id",
        f"SELECT * FROM mart_episodes WHERE anime_id = {anime_id} ORDER BY episode_id",
    )





# ── Plotly theme defaults ────────────────────────────────────────────────────

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Poppins, sans-serif", color="#ccc", size=12),
    margin=dict(l=40, r=20, t=45, b=40),
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)", zeroline=False),
    hoverlabel=dict(bgcolor="#1a1a2e", font_size=12, font_color="#f0f0f0"),
)

GOLD = "#e4a853"
GOLD_LIGHT = "#f7d794"
MUTED_RED = "#c0392b"


# ══════════════════════════════════════════════════════════════════════════════
# LAYOUT
# ══════════════════════════════════════════════════════════════════════════════

# ── Backend badge ─────────────────────────────────────────────────────────────
_, backend_type = get_backend()
st.markdown(
    f'<div class="backend-badge">📡 {backend_type.upper()}</div>',
    unsafe_allow_html=True,
)

# ── Marquee ───────────────────────────────────────────────────────────────────
top_posters = load_top_posters()
if top_posters:
    # Repeat items to make seamless infinite scroll looking full
    marquee_items = top_posters * 3
    img_tags = "".join([f'<img class="marquee-item" src="{url}">' for url in marquee_items])
    st.markdown(f'<div class="marquee-container"><div class="marquee-track">{img_tags}</div></div>', unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="header-container">
    <div class="header-title">🔍 OtakuLens</div>
    <p class="header-tagline">Your lens into the anime world</p>
</div>
<hr class="gold-divider">
""", unsafe_allow_html=True)

# ── Filters ───────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header" style="margin-top:0.5rem; margin-bottom:1rem;">🎛️ Filters</div>', unsafe_allow_html=True)

anime_list = load_anime_list()

sel_g_state = st.session_state.get("g_filter", [])
sel_t_state = st.session_state.get("t_filter", [])
sel_s_state = st.session_state.get("s_filter", [])
sel_m_state = st.session_state.get("m_filter", [])

def get_options(exclude_filter):
    idx = []
    for i, r in anime_list.iterrows():
        g_set = {r.get(c) for c in ["genre_1", "genre_2", "genre_3"] if pd.notna(r.get(c))}
        t_set = {r.get(c) for c in ["theme_1", "theme_2"] if pd.notna(r.get(c))}
        s_set = {s.strip() for s in str(r.get("studios", "")).split(",")} if pd.notna(r.get("studios")) else set()
        
        match = True
        m_val = r.get("anime_type")
        if exclude_filter != "g" and sel_g_state and not any(g in g_set for g in sel_g_state): match = False
        if match and exclude_filter != "t" and sel_t_state and not any(t in t_set for t in sel_t_state): match = False
        if match and exclude_filter != "s" and sel_s_state and not any(s in s_set for s in sel_s_state): match = False
        if match and exclude_filter != "m" and sel_m_state and m_val not in sel_m_state: match = False
        if match: idx.append(i)
        
    filtered_df = anime_list.iloc[idx]
    
    out_set = set()
    for _, r in filtered_df.iterrows():
        if exclude_filter == "g":
            for c in ["genre_1", "genre_2", "genre_3"]:
                if pd.notna(r.get(c)): out_set.add(r[c])
        elif exclude_filter == "t":
            for c in ["theme_1", "theme_2"]:
                if pd.notna(r.get(c)): out_set.add(r[c])
        elif exclude_filter == "s":
            if pd.notna(r.get("studios")):
                for s in str(r["studios"]).split(","): out_set.add(s.strip())
        elif exclude_filter == "m":
            if pd.notna(r.get("anime_type")): out_set.add(r["anime_type"])

    if exclude_filter == "g": out_set.update(sel_g_state)
    if exclude_filter == "t": out_set.update(sel_t_state)
    if exclude_filter == "s": out_set.update(sel_s_state)
    if exclude_filter == "m": out_set.update(sel_m_state)
    
    return sorted(out_set)

col_f1, col_f2, col_f3, col_f4 = st.columns(4)
with col_f1: sel_g = st.multiselect("Genres", get_options("g"), key="g_filter")
with col_f2: sel_t = st.multiselect("Themes", get_options("t"), key="t_filter")
with col_f3: sel_s = st.multiselect("Studios", get_options("s"), key="s_filter")
with col_f4: sel_m = st.multiselect("Media Type", get_options("m"), key="m_filter")

filtered_idx = []
for i, r in anime_list.iterrows():
    g_set = {r.get(c) for c in ["genre_1", "genre_2", "genre_3"] if pd.notna(r.get(c))}
    t_set = {r.get(c) for c in ["theme_1", "theme_2"] if pd.notna(r.get(c))}
    s_set = {s.strip() for s in str(r.get("studios", "")).split(",")} if pd.notna(r.get("studios")) else set()
    
    match = True
    m_val = r.get("anime_type")
    if sel_g and not any(g in g_set for g in sel_g): match = False
    if match and sel_t and not any(t in t_set for t in sel_t): match = False
    if match and sel_s and not any(s in s_set for s in sel_s): match = False
    if match and sel_m and m_val not in sel_m: match = False
    if match: filtered_idx.append(i)

anime_list = anime_list.iloc[filtered_idx].reset_index(drop=True)

# ── Anime selector ────────────────────────────────────────────────────────────
display_names = []
for _, r in anime_list.iterrows():
    name = r["title_english"] if pd.notna(r["title_english"]) and str(r["title_english"]).strip() else r["title"]
    display_names.append(name)
anime_list["display_name"] = display_names

if len(anime_list) == 0:
    st.warning("No anime match the selected filters.")
    st.stop()

col_sel, col_btn = st.columns([0.88, 0.12])
with col_sel:
    selected_idx = st.selectbox(
        "Select an anime",
        options=range(len(anime_list)),
        format_func=lambda i: anime_list.iloc[i]["display_name"],
        index=None,
        label_visibility="collapsed",
        placeholder="— Search or select an anime —",
        key="anime_sel"
    )

with col_btn:
    def clear_filters():
        for k in ["g_filter", "t_filter", "s_filter", "m_filter", "anime_sel"]:
            if k in st.session_state:
                if k == "anime_sel":
                    st.session_state[k] = None
                else:
                    st.session_state[k] = []
                    
    st.button("Clear", on_click=clear_filters, use_container_width=True)

if selected_idx is None:
    st.markdown("""
    <div class="welcome-container">
        <div class="welcome-icon">🔍</div>
        <div class="welcome-text">Select an anime above to explore its stats, characters, and episode insights.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

selected_anime_id = int(anime_list.iloc[selected_idx]["anime_id"])
row = load_anime_detail(selected_anime_id)

if row.empty:
    st.warning("No data found for this anime.")
    st.stop()

# ── Hero section ──────────────────────────────────────────────────────────────
col_img, col_info = st.columns([1, 2.5], gap="large")

with col_img:
    if pd.notna(row.get("image_url")):
        st.image(row["image_url"], width=240)

with col_info:
    en_title = row.get("title_english") if pd.notna(row.get("title_english")) else row.get("title", "")
    jp_title = row.get("title_japanese") if pd.notna(row.get("title_japanese")) else ""

    st.markdown(f'<div class="hero-title">{en_title}</div>', unsafe_allow_html=True)
    if jp_title:
        st.markdown(f'<div class="jp-title">{jp_title}</div>', unsafe_allow_html=True)

    # Metadata badges
    date_str = fmt_date(row.get("airing_start"))
    anime_type = row.get("anime_type", "—") or "—"
    studio = row.get("studios", "—") or "—"
    rating = row.get("rating", "—") or "—"
    status_val = row.get("status", "") or ""
    airing_icon = "🟢" if row.get("is_airing") else "🔴"
    badges = f"""
    <div class="meta-badges">
        <span>📅 {date_str}</span>
        <span>📺 {anime_type}</span>
        <span>🎬 {studio}</span>
        <span>⭐ {rating}</span>
        <span>{airing_icon} {status_val}</span>
    </div>
    """
    st.markdown(badges, unsafe_allow_html=True)

    # Genre + theme chips
    chips_html = '<div class="chip-row">'
    for g_col in ["genre_1", "genre_2", "genre_3"]:
        val = row.get(g_col)
        if val and pd.notna(val):
            chips_html += f'<span class="chip">{val}</span>'
    for t_col in ["theme_1", "theme_2"]:
        val = row.get(t_col)
        if val and pd.notna(val):
            chips_html += f'<span class="chip theme-chip">{val}</span>'
    chips_html += "</div>"
    st.markdown(chips_html, unsafe_allow_html=True)

    # Synopsis
    synopsis = row.get("synopsis", "")
    if synopsis and pd.notna(synopsis):
        synopsis_clean = re.sub(r"\s*\[Written by MAL Rewrite\]\s*", "", str(synopsis))
        synopsis_clean = synopsis_clean.strip().replace("\n", "<br>")
        st.markdown(f'<div class="synopsis-text">{synopsis_clean}</div>', unsafe_allow_html=True)


# ── Scorecards ────────────────────────────────────────────────────────────────
st.markdown("")  # spacer

score_val = row.get("avg_episode_score")
if score_val is None or pd.isna(score_val):
    score_val = row.get("score")
score_display = f"{score_val:.2f}" if pd.notna(score_val) and score_val is not None else "—"

pop_val = row.get("popularity_rank")
pop_display = f"#{int(pop_val)}" if pop_val and not pd.isna(pop_val) else "—"

watching_display = fmt_number(row.get("watching"))
_eps = row.get("total_episodes")
eps_display = str(int(_eps)) if _eps is not None and not pd.isna(_eps) else "—"

best_ep = row.get("best_episode_title", "")
best_ep_score = row.get("best_episode_score")
if best_ep and pd.notna(best_ep):
    best_ep_display = str(best_ep)
else:
    best_ep_display = "—"

cards = [
    ("⭐", score_display, "Score", False),
    ("📊", pop_display, "Popularity", False),
    ("👁️", watching_display, "Watching", False),
    ("📺", eps_display, "Episodes", False),
]

if str(row.get("status")) == "Finished Airing":
    comp = int(row.get("completed", 0)) if pd.notna(row.get("completed")) else 0
    drop = int(row.get("dropped", 0)) if pd.notna(row.get("dropped")) else 0
    if comp + drop > 0:
        cr = (comp / (comp + drop)) * 100
        cards.append(("🏁", f"{cr:.1f}%", "Completion", False))

cards.append(("🏅", best_ep_display, "Best Episode", True))

card_cols = st.columns(len(cards), gap="medium")
for col, (icon, value, label, is_small) in zip(card_cols, cards):
    with col:
        value_class = "card-value-small" if is_small else "card-value"
        st.markdown(f"""
        <div class="card">
            <div class="card-icon">{icon}</div>
            <div class="{value_class}">{value}</div>
            <div class="card-label">{label}</div>
        </div>
        """, unsafe_allow_html=True)


# ── Characters section ────────────────────────────────────────────────────────
characters_df = load_characters(selected_anime_id)

if len(characters_df) > 0:
    st.markdown('<div class="section-header">🎭 Main Characters</div>', unsafe_allow_html=True)

    chars_to_show = characters_df.sort_values("character_id").head(12)
    num_chars = len(chars_to_show)
    cols_per_row = 6
    rows_needed = (num_chars + cols_per_row - 1) // cols_per_row

    for r_idx in range(rows_needed):
        char_cols = st.columns(cols_per_row, gap="small")
        for c_idx in range(cols_per_row):
            flat_idx = r_idx * cols_per_row + c_idx
            if flat_idx < num_chars:
                char = chars_to_show.iloc[flat_idx]
                with char_cols[c_idx]:
                    # Format name: "Last, First" → "First Last"
                    name = str(char.get("name", ""))
                    if ", " in name:
                        parts = name.split(", ", 1)
                        name = f"{parts[1]} {parts[0]}"
                    
                    img_html = f'<img src="{char["image_url"]}" style="width:110px; height:155px; object-fit:cover;">' if pd.notna(char.get("image_url")) else '<div style="width:110px; height:155px; border:2px solid #1a1a2e; border-radius:10px;"></div>'
                    
                    st.markdown(f"""
                        <div class="char-card">
                            {img_html}
                            <div class="char-name">{name}</div>
                        </div>
                    """, unsafe_allow_html=True)


# ── Charts section ────────────────────────────────────────────────────────────
episodes_df = load_episodes(selected_anime_id)

is_episodic = row.get("anime_type") in ("TV", "ONA")

if len(episodes_df) > 0 or (row.get("watching") and not pd.isna(row.get("watching"))):
    st.markdown('<div class="section-header">📈 Analytics</div>', unsafe_allow_html=True)

    chart_left, chart_right = st.columns([1, 1] if is_episodic else [1, 0.001], gap="large")

    # ── Episode Score Timeline (TV / ONA only) ────────────────────────────────
    with chart_left:
        if not is_episodic:
            # Viewer engagement full-width for non-episodic types
            engagement_labels = ["Watching", "Completed", "On Hold", "Dropped", "Plan to Watch"]
            engagement_keys = ["watching", "completed", "on_hold", "dropped", "plan_to_watch"]
            engagement_values = [int(v) if (v := row.get(k)) is not None and pd.notna(v) else 0 for k in engagement_keys]
            if any(v > 0 for v in engagement_values):
                bar_colors = ["#e4a853", "#f7d794", "#9b8a5e", "#c0392b", "#636e72"]
                fig_eng = go.Figure()
                fig_eng.add_trace(go.Bar(
                    y=engagement_labels, x=engagement_values, orientation="h",
                    marker=dict(color=bar_colors, line=dict(width=0)),
                    hovertemplate="<b>%{y}</b><br>%{x:,}<extra></extra>",
                ))
                fig_eng.update_layout(
                    **PLOTLY_LAYOUT,
                    title=dict(text="Viewer Engagement", font=dict(size=15, color=GOLD)),
                    xaxis_title="Users",
                    yaxis=dict(autorange="reversed", gridcolor="rgba(255,255,255,0.05)", zeroline=False),
                    height=380, showlegend=False,
                )
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig_eng, use_container_width=True, config={"displayModeBar": False})
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.info("No engagement data available.")
        elif len(episodes_df) > 0:
            scored = episodes_df[episodes_df["score"].notna() & (episodes_df["score"] > 0)].copy()

            if len(scored) > 0:
                fig_ep = go.Figure()

                # Split filler vs non-filler
                non_filler = scored[scored["is_filler"] == False]  # noqa: E712
                filler = scored[scored["is_filler"] == True]  # noqa: E712

                if len(non_filler) > 0:
                    fig_ep.add_trace(go.Scatter(
                        x=non_filler["episode_id"],
                        y=non_filler["score"],
                        mode="lines+markers",
                        name="Canon",
                        line=dict(color=GOLD, width=2),
                        marker=dict(size=5, color=GOLD),
                        hovertemplate="<b>Ep %{x}</b><br>%{customdata}<br>Score: %{y:.2f}<extra></extra>",
                        customdata=non_filler["episode_title"],
                    ))

                if len(filler) > 0:
                    fig_ep.add_trace(go.Scatter(
                        x=filler["episode_id"],
                        y=filler["score"],
                        mode="lines+markers",
                        name="Filler",
                        line=dict(color=MUTED_RED, width=2, dash="dot"),
                        marker=dict(size=5, color=MUTED_RED),
                        hovertemplate="<b>Ep %{x} (Filler)</b><br>%{customdata}<br>Score: %{y:.2f}<extra></extra>",
                        customdata=filler["episode_title"],
                    ))

                fig_ep.update_layout(
                    **PLOTLY_LAYOUT,
                    title=dict(text="Episode Scores", font=dict(size=15, color=GOLD)),
                    xaxis_title="Episode",
                    yaxis=dict(title="Score", gridcolor="rgba(255,255,255,0.05)", zeroline=False),
                    legend=dict(
                        orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1, font=dict(size=11),
                    ),
                    height=380,
                )
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig_ep, use_container_width=True, config={"displayModeBar": False})
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.info("No scored episodes available.")
        else:
            st.info("No episode data available.")

    # ── Viewer Engagement (TV/ONA only — non-episodic shows it full-width above) ─
    with chart_right:
        if not is_episodic:
            pass  # already rendered full-width in chart_left
        else:
            engagement_labels = ["Watching", "Completed", "On Hold", "Dropped", "Plan to Watch"]
            engagement_keys = ["watching", "completed", "on_hold", "dropped", "plan_to_watch"]
            engagement_values = [int(v) if (v := row.get(k)) is not None and pd.notna(v) else 0 for k in engagement_keys]

            if any(v > 0 for v in engagement_values):
                bar_colors = ["#e4a853", "#f7d794", "#9b8a5e", "#c0392b", "#636e72"]
                fig_eng = go.Figure()
                fig_eng.add_trace(go.Bar(
                    y=engagement_labels,
                    x=engagement_values,
                    orientation="h",
                    marker=dict(color=bar_colors, line=dict(width=0)),
                    hovertemplate="<b>%{y}</b><br>%{x:,}<extra></extra>",
                ))
                fig_eng.update_layout(
                    **PLOTLY_LAYOUT,
                    title=dict(text="Viewer Engagement", font=dict(size=15, color=GOLD)),
                    xaxis_title="Users",
                    yaxis=dict(autorange="reversed", gridcolor="rgba(255,255,255,0.05)", zeroline=False),
                    height=380,
                    showlegend=False,
                )
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig_eng, use_container_width=True, config={"displayModeBar": False})
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.info("No engagement data available.")


# ── Row 2: Filler vs Canon + Episode Score Distribution (episodic only) ───────
if is_episodic:
    st.markdown('<div class="section-header">📊 Breakdown</div>', unsafe_allow_html=True)
    chart_left2, chart_right2 = st.columns(2, gap="large")

    with chart_left2:
        if len(episodes_df) > 0:
            filler_count = int(row.get("filler_count", 0)) if pd.notna(row.get("filler_count")) else 0
            total_eps = int(row.get("total_episodes", 0)) if pd.notna(row.get("total_episodes")) else 0
            canon_count = max(total_eps - filler_count, 0)
            fig_fc = go.Figure()
            fig_fc.add_trace(go.Pie(
                labels=["Canon", "Filler"],
                values=[canon_count, filler_count],
                hole=0.55,
                marker=dict(colors=[GOLD, MUTED_RED]),
                textinfo="label+percent",
                textfont=dict(size=13, color="#f0f0f0"),
                hovertemplate="<b>%{label}</b><br>%{value} episodes (%{percent})<extra></extra>",
            ))
            fig_fc.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Poppins, sans-serif", color="#ccc", size=12),
                title=dict(text="Filler vs Canon", font=dict(size=15, color=GOLD)),
                margin=dict(l=20, r=20, t=50, b=20),
                hoverlabel=dict(bgcolor="#1a1a2e", font_size=12, font_color="#f0f0f0"),
                height=360,
                showlegend=False,
                annotations=[dict(
                    text=f"<b>{total_eps}</b><br>eps",
                    x=0.5, y=0.5, font=dict(size=18, color="#f0f0f0"),
                    showarrow=False,
                )],
            )
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig_fc, use_container_width=True, config={"displayModeBar": False})
            st.markdown("</div>", unsafe_allow_html=True)
        else:
            st.info("No episode data available.")

    with chart_right2:
        if len(episodes_df) > 0:
            scored_eps = episodes_df[episodes_df["score"].notna() & (episodes_df["score"] > 0)].copy()
            if len(scored_eps) > 0:
                fig_hist = go.Figure()
                fig_hist.add_trace(go.Histogram(
                    x=scored_eps["score"],
                    nbinsx=15,
                    marker=dict(color=GOLD, line=dict(width=1, color="#1a1a2e")),
                    hovertemplate="Score: %{x:.1f}<br>Episodes: %{y}<extra></extra>",
                ))
                avg_score = scored_eps["score"].mean()
                fig_hist.add_vline(x=avg_score, line=dict(color=MUTED_RED, width=2, dash="dash"))
                fig_hist.add_annotation(
                    x=avg_score, y=0.9, yref="paper",
                    text=f"<b>Avg: {avg_score:.2f}</b>",
                    showarrow=True, arrowhead=2, arrowsize=1, arrowwidth=2,
                    arrowcolor=MUTED_RED, ax=35, ay=-25,
                    font=dict(size=13, color="#f0f0f0"),
                    bgcolor=MUTED_RED, borderpad=4, bordercolor=MUTED_RED,
                )
                fig_hist.update_layout(
                    **PLOTLY_LAYOUT,
                    title=dict(text="Episode Score Distribution", font=dict(size=15, color=GOLD)),
                    xaxis_title="Score",
                    yaxis=dict(title="Episodes", gridcolor="rgba(255,255,255,0.05)", zeroline=False),
                    height=360, showlegend=False, bargap=0.08,
                )
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig_hist, use_container_width=True, config={"displayModeBar": False})
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                st.info("No scored episodes available.")
        else:
            st.info("No episode data available.")


# ── Similar Anime ─────────────────────────────────────────────────────────────
st.markdown('<div class="section-header">🔍 Similar Anime</div>', unsafe_allow_html=True)

all_anime_df = load_all_anime_for_similarity()
model = load_embedding_model()
embeddings = compute_embeddings(model, all_anime_df)
similar = get_similar_anime(selected_anime_id, all_anime_df, embeddings)

if not similar.empty:
    cols = st.columns(6)
    for col, (_, r) in zip(cols, similar.iterrows()):
        with col:
            if pd.notna(r.get("image_url")):
                st.image(r["image_url"], use_container_width=True)
            label = r.get("title_english") or r.get("title") or ""
            score = f"⭐ {r['score']:.1f}" if pd.notna(r.get("score")) else ""
            st.caption(f"**{label}**  \n{score}")


# ── Footer spacer ─────────────────────────────────────────────────────────────
st.markdown("<br><br>", unsafe_allow_html=True)
