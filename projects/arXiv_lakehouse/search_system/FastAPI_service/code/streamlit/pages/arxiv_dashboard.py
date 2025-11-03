# arxiv_dashboard.py
import itertools
from datetime import date
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
import networkx as nx

# ---- optional: for interactive network graph ----
try:
    from pyvis.network import Network
    HAS_PYVIS = True
except Exception:
    HAS_PYVIS = False

# ---------- DB CONNECTION (reuse your pattern) ----------
from mysql.connector import connect

def mysql_connection():
    return connect(
        host='mysql_db_container',  # adjust if needed
        port=3306,
        user='root',
        password='!QAZ2wsx',
        database='arXiv',
        connection_timeout=600
    )

@st.cache_data(ttl=300)
def load_tables():
    conn = mysql_connection()
    papers = pd.read_sql("SELECT * FROM papers", conn)
    authors = pd.read_sql("SELECT * FROM authors", conn)
    versions = pd.read_sql("SELECT * FROM versions", conn)
    coauthorship = pd.read_sql("SELECT * FROM coauthorship_edges LIMIT 1000", conn)
    coauthor_stats = pd.read_sql("SELECT * FROM coauthor_stats ORDER BY degree DESC LIMIT 20", conn)
    conn.close()

    return papers, authors, versions, coauthorship, coauthor_stats

def primary_category(cat: str) -> str:
    # arXiv categories are space-separated; take the first (primary) code
    if not isinstance(cat, str) or not cat.strip():
        return "(unknown)"
    return cat.strip().split()[0]

# =========================================================
# 1) 📈 Average number of updates (versions) per discipline
# =========================================================
def compute_avg_updates_by_category(papers: pd.DataFrame, versions: pd.DataFrame) -> pd.DataFrame:
    # count versions per paper
    vcnt = versions.groupby("paper_id", as_index=False).size().rename(columns={"size": "version_count"})
    base = papers[["id", "categories"]].copy()
    base["category"] = base["categories"].apply(primary_category)
    out = base.merge(vcnt, left_on="id", right_on="paper_id", how="left")
    out["version_count"] = out["version_count"].fillna(0).astype(int)
    # average versions per paper per category
    agg = out.groupby("category", as_index=False)["version_count"].mean()
    agg = agg.sort_values("version_count", ascending=False).rename(columns={"version_count": "avg_versions"})
    return agg

def chart_avg_updates(df: pd.DataFrame, top_n=20):
    data = df.head(top_n)
    c = (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X("avg_versions:Q", title="Average versions per paper"),
            y=alt.Y("category:N", sort="-x", title="Discipline (primary category)"),
            tooltip=["category", alt.Tooltip("avg_versions:Q", format=".2f")]
        )
        .properties(height=30*len(data), title="Average number of updates per discipline")
    )
    st.altair_chart(c, use_container_width=True)

# =========================================================
# 2) ⏳ Median time from submission → publication (weekly)
# =========================================================
def compute_weekly_median_time_to_pub(papers: pd.DataFrame, versions: pd.DataFrame) -> pd.DataFrame:
    # Robust: try to use 'submitted' column if present; else first version.created
    df = papers[["id", "categories", "published"]].copy()
    df["category"] = df["categories"].apply(primary_category)

    # parse dates
    for col in ["published"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    have_submitted = "submitted" in papers.columns
    if have_submitted:
        sub = papers[["id", "submitted"]].copy()
        sub["submitted"] = pd.to_datetime(sub["submitted"], errors="coerce")
        df = df.merge(sub, on="id", how="left")
    else:
        # derive submitted as first version.created per paper
        vv = versions[["paper_id", "created"]].copy()
        vv["created"] = pd.to_datetime(vv["created"], errors="coerce")
        first_v = vv.groupby("paper_id", as_index=False)["created"].min().rename(columns={"created": "submitted"})
        df = df.merge(first_v, left_on="id", right_on="paper_id", how="left")

    df["submitted"] = pd.to_datetime(df["submitted"], errors="coerce")
    df = df.dropna(subset=["submitted", "published"]).copy()

    df["days_to_pub"] = (df["published"] - df["submitted"]).dt.days
    # group by week (of submitted date) and category
    df["week"] = df["submitted"].dt.to_period("W").apply(lambda r: r.start_time.date())
    med = (
        df.groupby(["category", "week"], as_index=False)["days_to_pub"]
        .median()
        .rename(columns={"days_to_pub": "median_days"})
    )
    return med

def chart_weekly_median(df: pd.DataFrame, pick_categories: list = None):
    data = df.copy()
    if pick_categories:
        data = data[data["category"].isin(pick_categories)]
    c = (
        alt.Chart(data)
        .mark_line(point=True)
        .encode(
            x=alt.X("week:T", title="Week (submission week)"),
            y=alt.Y("median_days:Q", title="Median days to publication"),
            color=alt.Color("category:N", title="Discipline"),
            tooltip=["category", "week", "median_days"]
        )
        .properties(height=400, title="Weekly median time: submission → publication")
    )
    st.altair_chart(c, use_container_width=True)

# =========================================================
# 3) 🧮 Cumulative submissions per author (or institution)
# =========================================================
def compute_cumulative_by_author(papers: pd.DataFrame, authors: pd.DataFrame) -> pd.DataFrame:
    base = authors.merge(
        papers[["id", "published"]].rename(columns={"id": "paper_id"}),
        on="paper_id", how="left"
    )
    base["published"] = pd.to_datetime(base["published"], errors="coerce").dt.date
    base = base.dropna(subset=["published"])
    base = base.rename(columns={"name": "author"})
    # cumulative per author over time
    base = base.sort_values(["author", "published"])
    base["cum_submissions"] = base.groupby("author").cumcount() + 1
    return base[["author", "published", "cum_submissions"]]

def chart_cumulative_by_author(df: pd.DataFrame, authors_to_show: list, resample="W"):
    if not authors_to_show:
        st.info("Select at least one author to visualize.")
        return
    dat = df[df["author"].isin(authors_to_show)].copy()
    # upsamples weekly; forward-fill cum count
    dat["published"] = pd.to_datetime(dat["published"])
    out = []
    for a, grp in dat.groupby("author"):
        ts = (
            grp.set_index("published")["cum_submissions"]
               .resample(resample)
               .max()
               .ffill()
               .reset_index()
        )
        ts["author"] = a
        out.append(ts)
    plot = pd.concat(out, ignore_index=True)
    c = (
        alt.Chart(plot)
        .mark_line(point=True)
        .encode(
            x=alt.X("published:T", title="Date"),
            y=alt.Y("cum_submissions:Q", title="Cumulative submissions"),
            color="author:N",
            tooltip=["author", "published", "cum_submissions"]
        )
        .properties(height=400, title="Cumulative submissions per author")
    )
    st.altair_chart(c, use_container_width=True)

# If you have an institutions field (e.g., authors.affiliation), you can duplicate the function:
def compute_cumulative_by_institution(papers: pd.DataFrame, authors: pd.DataFrame) -> pd.DataFrame:
    if "affiliation" not in authors.columns:
        return pd.DataFrame(columns=["affiliation", "published", "cum_submissions"])
    base = authors.merge(
        papers[["id", "published"]].rename(columns={"id": "paper_id"}),
        on="paper_id", how="left"
    )
    base["published"] = pd.to_datetime(base["published"], errors="coerce").dt.date
    base = base.dropna(subset=["published"]).rename(columns={"affiliation": "institution"})
    base = base.sort_values(["institution", "published"])
    base["cum_submissions"] = base.groupby("institution").cumcount() + 1
    return base[["institution", "published", "cum_submissions"]]

# =========================================================
# 4) 🔗 Academic co-authorship network
# =========================================================
@st.cache_data(ttl=600)
def build_coauthorship_edges(authors: pd.DataFrame) -> pd.DataFrame:
    # edges: pairs of authors who co-appear on the same paper_id
    edges = []
    for pid, grp in authors.groupby("paper_id"):
        names = sorted(set(grp["name"].dropna().astype(str)))
        for a, b in itertools.combinations(names, 2):
            edges.append((a, b))
    if not edges:
        return pd.DataFrame(columns=["source", "target", "weight"])
    edf = pd.DataFrame(edges, columns=["source", "target"])
    edf["weight"] = 1
    edf = edf.groupby(["source", "target"], as_index=False)["weight"].sum()
    return edf

def networkx_from_edges(edf: pd.DataFrame) -> nx.Graph:
    G = nx.Graph()
    for _, r in edf.iterrows():
        G.add_edge(r["source"], r["target"], weight=float(r["weight"]))
    return G

def show_coauthor_stats(G: nx.Graph, top_k=20):
    dc = nx.degree_centrality(G)
    bc = nx.betweenness_centrality(G, k=min(1000, len(G))) if len(G) > 1000 else nx.betweenness_centrality(G)
    df = pd.DataFrame({
        "author": list(G.nodes()),
        "degree": [G.degree(n) for n in G.nodes()],
        "degree_centrality": [dc[n] for n in G.nodes()],
        "betweenness": [bc[n] for n in G.nodes()],
    }).sort_values(["degree", "betweenness"], ascending=False)
    st.markdown("**Top authors by degree & betweenness**")
    st.dataframe(df.head(top_k), width='stretch')

def show_pyvis_network(edf: pd.DataFrame, min_weight=1):
    if not HAS_PYVIS or edf.empty:
        st.info("PyVis not installed or no edges. Showing edge table.")
        st.dataframe(edf.sort_values("weight", ascending=False).head(200))
        return
    net = Network(height="640px", width="100%", notebook=False, directed=False, cdn_resources="in_line")
    # only keep edges with at least min_weight
    edff = edf[edf["weight"] >= min_weight]
    # add nodes
    nodes = set(edff["source"]).union(set(edff["target"]))
    for n in nodes:
        net.add_node(n, label=n)
    # add edges
    for _, r in edff.iterrows():
        net.add_edge(r["source"], r["target"], value=float(r["weight"]))
    net.force_atlas_2based(gravity=-50)
    html_path = "/tmp/coauthor_network.html"
    net.save_graph(html_path)
    st.markdown(f"[Open interactive network in a new tab]({html_path})")
    st.components.v1.html(open(html_path, "r", encoding="utf-8").read(), height=660, scrolling=True)

# =========================
#          UI
# =========================
st.set_page_config(page_title="arXiv Analytics Dashboard", layout="wide")
st.title("arXiv Analytics Dashboard")

with st.spinner("Loading data from MySQL…"):
    papers, authors, versions, edges, coauthor_stats = load_tables()

# --- precompute common fields
papers["category"] = papers["categories"].apply(primary_category)

# ========== 1) Average updates ==========
st.header("📈 Average number of updates per discipline")
avg_df = compute_avg_updates_by_category(papers, versions)
top_n = st.slider("Show top N disciplines", 5, 50, 20)
chart_avg_updates(avg_df, top_n=top_n)
st.dataframe(avg_df, width='stretch')

st.markdown("---")

# ========== 2) Weekly median time ==========
st.header("⏳ Weekly median time: submission → publication")
med_df = compute_weekly_median_time_to_pub(papers, versions)
categories_for_trend = st.multiselect(
    "Pick disciplines to visualize",
    options=sorted(med_df["category"].unique().tolist()),
    default=sorted(med_df["category"].unique().tolist()[:5])
)
chart_weekly_median(med_df, pick_categories=categories_for_trend)
st.dataframe(med_df.sort_values(["category", "week"]), width='stretch')

st.markdown("---")

# ========== 3) Cumulative submissions ==========
st.header("🧮 Cumulative submissions over time")
cum_author = compute_cumulative_by_author(papers, authors)
# suggest top prolific authors
top_authors = (
    cum_author.groupby("author").size().sort_values(ascending=False).head(20).index.tolist()
)
authors_to_show = st.multiselect("Authors to plot (top suggestions shown)", options=sorted(cum_author["author"].unique()), default=top_authors[:5])
chart_cumulative_by_author(cum_author, authors_to_show=authors_to_show, resample="W")
st.dataframe(cum_author[cum_author["author"].isin(authors_to_show)].sort_values(["author","published"]), width='stretch')

# (Optional) institution variant if you have authors.affiliation
if "affiliation" in authors.columns:
    st.subheader("Institution view (optional)")
    cum_inst = compute_cumulative_by_institution(papers, authors)
    if not cum_inst.empty:
        inst_suggest = (
            cum_inst.groupby("institution").size().sort_values(ascending=False).head(20).index.tolist()
        )
        pick_inst = st.multiselect("Institutions to show", options=sorted(cum_inst["institution"].unique()), default=inst_suggest[:5])
        if pick_inst:
            dat = cum_inst[cum_inst["institution"].isin(pick_inst)].copy()
            dat["published"] = pd.to_datetime(dat["published"])
            out = []
            for ins, grp in dat.groupby("institution"):
                ts = grp.set_index("published")["cum_submissions"].resample("W").max().ffill().reset_index()
                ts["institution"] = ins
                out.append(ts)
            plot = pd.concat(out, ignore_index=True)
            c = (
                alt.Chart(plot)
                .mark_line(point=True)
                .encode(
                    x=alt.X("published:T", title="Date"),
                    y=alt.Y("cum_submissions:Q", title="Cumulative submissions"),
                    color="institution:N",
                    tooltip=["institution", "published", "cum_submissions"]
                )
                .properties(height=400, title="Cumulative submissions per institution")
            )
            st.altair_chart(c, use_container_width=True)
            st.dataframe(dat.sort_values(["institution","published"]), width='stretch')
    else:
        st.info("No `affiliation` field detected with data.")

st.markdown("---")

# ========== 4) Co-authorship network ==========
st.header("🔗 Academic co-authorship network")
# edges = build_coauthorship_edges(authors)
st.caption(f"Edges (pairs of authors who co-authored at least one paper). Unique edges: {len(edges)}")
min_w = st.slider("Minimum co-authored papers to keep an edge", 1, 10, 1)
G = networkx_from_edges(edges[edges["weight"] >= min_w]) if not edges.empty else nx.Graph()

# stats / tables
if len(G) == 0:
    st.info("No co-authorship edges found.")
else:
    colA, colB = st.columns([1,1])
    # with colA:
    #     show_coauthor_stats(G, top_k=20)
    with colB:
        st.markdown("**Edge sample**")
        # st.dataframe(edges.sort_values("weight", ascending=False).head(200), width='stretch')
        st.dataframe(coauthor_stats)
    st.markdown("**Interactive network (PyVis)**")
    show_pyvis_network(edges, min_weight=min_w)
