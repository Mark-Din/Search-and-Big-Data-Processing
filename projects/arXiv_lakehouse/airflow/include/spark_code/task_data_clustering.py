import os
import time
import numpy as np
import joblib
import boto3
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import MiniBatchKMeans
from scipy.sparse import csr_matrix
from elasticsearch import Elasticsearch, helpers
from sparksession import spark_session
from include.mysql_log import store_metadata
from include.init_log import initlog

from es_mapping import arxiv_mapping

from include.category import category_map


logger = initlog(__name__)

# ------------------------------
# Read GOLD layer
# ------------------------------
def read_gold(spark):
    GOLD = os.getenv("GOLD_PATH", "s3a://deltabucket/gold/arxiv_features")
    return spark.read.format("delta").load(GOLD)

# ------------------------------
# Write results to Elasticsearch
# ------------------------------
def to_elasticsearch(ids, titles, abstracts, categories, version, version_created, updated, published, comments, clusters, vectors, es_index="arxiv_clusters"):
    """Bulk insert into Elasticsearch with dense_vector mapping."""
    es = Elasticsearch(
        hosts=["http://elasticsearch:9200"],
        basic_auth=("elastic", "gAcstb8v-lFCVzCBC__a"),  # adjust if you have auth
        verify_certs=False,
    )
    
    # Ensure index exists with proper mapping
    if not es.indices.exists(index=es_index):
        mapping = arxiv_mapping()
        es.indices.create(index=es_index, body=mapping)
        print(f"✅ Created Elasticsearch index '{es_index}' with mapping.")

    # Prepare bulk actions
    # Ensure vectors are in list of floats format
    vectors = [[float(x) if np.isfinite(x) else 0.0 for x in v] for v in vectors]

    actions = []
    for pid, t, a, c, v, v_c, u, p, com, cl, vec in zip(ids, titles, abstracts, categories, version, version_created, updated, published, comments, clusters, vectors):
        doc = {
            "_op_type": "index",
            "_index": es_index,
            "_id": pid,
            "_source": {
                "paper_id": pid,
                "title": t,
                "abstract": a,
                "categories": '/'.join([category_map[word] if category_map.get(word, None) else word for word in c.split()]),
                "version": v,
                "version_created": v_c,
                "updated": u,
                "published": p,
                "comments": com,
                "cluster": int(cl),
                "vector": vec,  # dense_vector field
            },
        }
        actions.append(doc)

    if actions:
        helpers.bulk(es, actions)
        print(f"✅ Inserted {len(actions)} docs into Elasticsearch index '{es_index}'")


# ------------------------------
# Core clustering logic
# ------------------------------
def fit_predict(data_whole, sample_frac=0.01, sample_cap=5000, batch_size=10000):
    """
    Run SVD + MiniBatchKMeans on a manageable sample and write results to Elasticsearch.
    """

    # --- get feature size from one row
    first_vec = data_whole.limit(1).collect()[0]["features"]
    num_features = int(first_vec.size)

    # --- helper: Spark Row -> CSR batch
    def to_csr(rows):
        indptr = [0]
        indices = []
        vals = []
        for sv in rows:
            indices.extend(sv.indices.tolist())
            vals.extend(sv.values.tolist())
            indptr.append(indptr[-1] + len(sv.indices))
        return csr_matrix(
            (np.array(vals, dtype=np.float64),
             np.array(indices, dtype=np.int32),
             np.array(indptr, dtype=np.int32)),
            shape=(len(rows), num_features)
        )

    # 1) Collect a sample for training
    sample_rows = []
    for i, row in enumerate(data_whole.sample(False, sample_frac, seed=42).toLocalIterator()):
        sample_rows.append(row["features"])
        if i >= sample_cap:
            break

    X_sample = to_csr(sample_rows)
    svd = TruncatedSVD(n_components=100, random_state=42).fit(X_sample)

    # 2) Train KMeans (incremental)
    kmeans = MiniBatchKMeans(
        n_clusters=20,
        random_state=42,
        batch_size=batch_size,
        verbose=1,
        n_init="auto"
    )

    batch_features, batch_ids, batch_titles, batch_abstracts, batch_categories, batch_version, batch_version_created, batch_updated, batch_published, batch_comments = [], [], [], [], [], [], [], [], [], []
    count = 0

    # --- Training phase ---
    for row in data_whole.select("paper_id", "title", "abstract", "categories", "version", "version_created", "updated", "published", "comments", "features").toLocalIterator():
        batch_features.append(row["features"])
        batch_ids.append(row["paper_id"])
        batch_titles.append(row["title"])
        batch_abstracts.append(row["abstract"])
        batch_categories.append(row["categories"])
        batch_version.append(row["version"])
        batch_version_created.append(row["version_created"])
        batch_updated.append(row["updated"])
        batch_published.append(row["published"])
        batch_comments.append(row["comments"])

        if len(batch_features) >= batch_size:
            Xb = to_csr(batch_features)
            Xr = svd.transform(Xb)
            kmeans.partial_fit(Xr)

            batch_features.clear()
            batch_ids.clear()
            batch_titles.clear()
            batch_abstracts.clear()
            batch_categories.clear()
            count += batch_size

    # Train on leftovers
    if batch_features:
        Xb = to_csr(batch_features)
        Xr = svd.transform(Xb)
        kmeans.partial_fit(Xr)

    print("✅ KMeans model trained.")


    # 3) Prediction phase: insert into Elasticsearch
    batch_features, batch_ids, batch_titles, batch_abstracts, batch_categories, batch_version, batch_version_created, batch_updated, batch_published, batch_comments = [], [], [], [], [], [], [], [], [], []
    batch_count = 1

    for row in data_whole.select(
        "paper_id", "title", "abstract", "categories", "version",
        "version_created", "updated", "published", "comments", "features"
    ).toLocalIterator():
        batch_features.append(row["features"])
        batch_ids.append(row["paper_id"])
        batch_titles.append(row["title"])
        batch_abstracts.append(row["abstract"])
        batch_categories.append(row["categories"])
        batch_version.append(row["version"])
        batch_version_created.append(row["version_created"])
        batch_updated.append(row["updated"])
        batch_published.append(row["published"])
        batch_comments.append(row["comments"])

        if len(batch_features) >= batch_size:
            Xb = to_csr(batch_features)
            Xr = svd.transform(Xb)
            preds = kmeans.predict(Xr)
            vectors = Xr.tolist()

            print("Vector sample length:", len(vectors[0]))

            print(f"Inserting batch {batch_count} into Elasticsearch...")
            to_elasticsearch(batch_ids, batch_titles, batch_abstracts, batch_categories, batch_version, batch_version_created, batch_updated, batch_published, batch_comments, preds, vectors)

            batch_features.clear()
            batch_ids.clear()
            batch_titles.clear()
            batch_abstracts.clear()
            batch_categories.clear()
            batch_count += 1

    # Predict leftovers
    if batch_features:
        Xb = to_csr(batch_features)
        Xr = svd.transform(Xb)
        preds = kmeans.predict(Xr)
        vectors = Xr.tolist()
        print("Vector sample length:", len(vectors[0]))

        to_elasticsearch(batch_ids, batch_titles, batch_abstracts, batch_categories, batch_version, batch_version_created, batch_updated, batch_published, batch_comments, preds, vectors)

    print("🎉 All documents inserted into Elasticsearch.")
    return svd, kmeans, 


# ------------------------------
# Save models to MinIO
# ------------------------------
def save_models(kmeans, svd):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    # save locally
    joblib.dump(svd, "/tmp/svd.pkl")
    joblib.dump(kmeans, "/tmp/kmeans.pkl")

    # upload to MinIO
    s3.upload_file("/tmp/svd.pkl", "deltabucket", "models/arxiv_svd.pkl")
    s3.upload_file("/tmp/kmeans.pkl", "deltabucket", "models/arxiv_kmeans.pkl")
    print("✅ Models saved to MinIO.")


# ------------------------------
# Main
# ------------------------------
def main():
    start = time.time()

    s = None
    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
    status = 'F'

    try:
        logger.info("🚀 Starting clustering process...")
        s = spark_session()
        df = read_gold(s)

        logger.info(">>> Running SVD + KMeans ...")
        svd, kmeans = fit_predict(df)

        logger.info(">>> Saving models ...")
        save_models(kmeans, svd)

        logger.info("✅ All done.")

        len_df = df.count()
        logger.info("Gold feature build complete.")

        status = 'S'

    except Exception as e:
        logger.error(f"❌ Error in clustering job: {e}", exc_info=True)
    finally:
        if s:
            s.stop()
            logger.info("Spark session stopped.")
        try:
            
            end = time.time()
            duration = end - start if 'start' in locals() else 0

            store_metadata(
                run_id=run_id,
                stage_name='data_clustering',
                record_count=len_df if 'df' in locals() else 0,
                duration=duration if 'duration' in locals() else 0,
                status=status,
                component='spark',
                note='Data clustering job completed'
            )
        except Exception as me:
            logger.error(f"❌ Error storing metadata: {me}", exc_info=True)
            
if __name__ == "__main__":
    main()
