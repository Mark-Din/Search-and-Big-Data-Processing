import time
import pandas as pd
import itertools
from datetime import datetime
from include.init_log import initlog
from include.mysql_log import store_metadata
from include.connection import ElasticSearchConnectionManager as em

import networkx as nx
import networkit as nk

logger = initlog(__name__)

conn = em.mysql_connection()

def custom_degree_centrality(G):

    G_nk = nk.graph.Graph(directed=False)
    id_map = {}  # map your author names to ints
    for i, node in enumerate(G.nodes()):
        id_map[node] = i
        G_nk.addNode()
    for u, v in G.edges():
        G_nk.addEdge(id_map[u], id_map[v])

    approx_bc = nk.centrality.ApproxBetweenness(G_nk)
    approx_bc.run()
    scores = approx_bc.scores()

    bc = {}
    for i,value in id_map.items():
        bc[i] = scores[value]
    
    return bc

def main():
    global LEN_DF
    LEN_DF = 0

    # Load data
    authors = pd.read_sql("SELECT paper_id, name FROM authors", conn)

    logger.info(f'authors:===== {authors.count()}')

    def clean_name(name: str) -> str:
        if not isinstance(name, str):
            return ""
        name = name.replace("\n", " ").replace("\r", " ").strip()
        name = " ".join(name.split())  # normalize multiple spaces
        return name

    # Apply before combinations
    authors["name"] = authors["name"].apply(clean_name)

    # Build coauthorship edges
    edges = []
    for pid, grp in authors.groupby("paper_id"):
        names = sorted(set(grp["name"].dropna().astype(str)))
        for a, b in itertools.combinations(names, 2):
            edges.append((a, b))

    edges_df = pd.DataFrame(edges, columns=["source", "target"])
    edges_df["weight"] = 1
    edges_df = edges_df.groupby(["source", "target"], as_index=False)["weight"].sum()
    edges_df["updated_at"] = datetime.now()

    # Clear + insert new edges
    cursor = conn.cursor()

    edges_df.drop_duplicates(["source", "target"],inplace=True)
    LEN_DF = len(edges_df)
    
    logger.info(f"Total coauthorship edges to insert: {LEN_DF}")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS coauthorship_edges (
        source VARCHAR(255),
        target VARCHAR(255),
        weight INT,
        updated_at DATETIME,
        PRIMARY KEY (source, target)
        );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS coauthor_stats (
        author VARCHAR(255) PRIMARY KEY,
        degree INT,
        degree_centrality FLOAT,
        betweenness FLOAT,
        updated_at DATETIME
        );
    """)
    conn.commit()

    cursor.execute("DELETE FROM coauthorship_edges")
    conn.commit()

    for _, r in edges_df.iterrows():
        cursor.execute("""
                INSERT INTO coauthorship_edges (source, target, weight, updated_at)
                VALUES (%s, %s, %s, %s)
        """, (r.source, r.target, int(r.weight), r.updated_at.to_pydatetime()))
    conn.commit()

    # # Compute and store network stats
    G = nx.from_pandas_edgelist(edges_df, "source", "target", edge_attr="weight")

    logger.info('Computing centrality')

    dc = nx.degree_centrality(G)
    # bc = nx.betweenness_centrality(G, k=min(1000, len(G))) # This line is taking too much time, around 3 million rows for 11 mins.

    bc = custom_degree_centrality(G)

    stats = [
        (a, G.degree(a), dc[a], bc[a], datetime.now())
        for a in G.nodes()
    ]

    logger.info(f"Inserting coauthorship stats into the database... ,count: [{len(stats)}]")    
    
    cursor.execute("DELETE FROM coauthor_stats")
    for row in stats:
        cursor.execute("""
            INSERT INTO coauthor_stats (author, degree, degree_centrality, betweenness, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                degree = VALUES(degree),
                degree_centrality = VALUES(degree_centrality),
                betweenness = VALUES(betweenness),
                updated_at = VALUES(updated_at)
        """, row)
        
    conn.commit()
    conn.close()

    logger.info("✅ ETL completed successfully!")

if __name__ == '__main__':

    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
    try:
        start = time.time()

        main()
        end = time.time()
        duration = end - start   # in seconds (float)
        status = 'S'
    except Exception as e:
        logger.error(f"ETL process failed {e}", exc_info=True)
        status = 'F'
    finally:
        try:
            store_metadata(
                run_id=run_id,
                stage_name='etl_api_to_mysql',
                component='python',
                record_count=LEN_DF if 'count' in locals() else 0,
                duration=duration if 'duration' in locals() else 0,
                status=status,
                note='ETL job from arXiv api to MySQL'
            )
        except Exception as e:
            logger.error(f"❌ Error storing metadata: {e}", exc_info=True)