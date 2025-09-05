import os
from pyspark.sql import SparkSession
from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import MiniBatchKMeans
from scipy.sparse import csr_matrix
import numpy as np
import joblib, boto3

from sklearn.random_projection import SparseRandomProjection
from sklearn.neighbors import NearestNeighbors

from scipy.sparse import vstack as sp_vstack

from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, IntegerType, StructType, StructField, ArrayType, DoubleType

from pyspark.ml.linalg import SparseVector, DenseVector

import pandas as pd



def read_gold(spark):
    global OUT

    GOLD = os.getenv("GOLD_PATH","s3a://deltabucket/gold/wholeCorp_delta")
    OUT = os.getenv("CLUSTER_PATH","s3a://deltabucket/gold/wholeCorp_clusters")

    return spark.read.format("delta").load(GOLD)


def fit_predict(data_whole):
    # --- get feature size from one row
    first_vec = data_whole.limit(1).collect()[0][2]
    num_features = int(first_vec.size)

    # --- helper: Spark Row -> CSR batch
    def to_csr(rows):
        indptr = [0]; indices = []; vals = []
        for sv in rows:
            indices.extend(sv.indices.tolist())
            vals.extend(sv.values.tolist())
            indptr.append(indptr[-1] + len(sv.indices))
        return csr_matrix((np.array(vals, dtype=np.float64),
                        np.array(indices, dtype=np.int32),
                        np.array(indptr, dtype=np.int32)),
                        shape=(len(rows), num_features
    ))

    # 1) collect a manageable sample from Spark
    sample_rows = []
    for i, row in enumerate(data_whole.sample(False, 0.02, seed=42).toLocalIterator()):  # ~2% example
        sample_rows.append(row['features'])
        if i >= 20000:      # cap by count if you like
            break

    X_sample = to_csr(sample_rows)            # CSR (n_sample, num_features)
    print(X_sample)
    
    svd = TruncatedSVD(n_components=100, random_state=42).fit(X_sample)

    # --- Stage 2: fit MiniBatchKMeans on reduced features
    kmeans = MiniBatchKMeans(n_clusters=15,
                            random_state=42,
                            batch_size=2000,
                            verbose=1,
                            n_init='auto')
    ids, labels = [], []
    batch_features, batch_ids = [], []

    Xr_vector = []
    for row in data_whole.select("統一編號","features").toLocalIterator():
        batch_features.append(row["features"])
        batch_ids.append(row["統一編號"])
        if len(batch_features) >= 2000:
            Xb = to_csr(batch_features)
            Xr = svd.transform(Xb)
            Xr_vector.extend(Xr)
            
            kmeans.partial_fit(Xr)
            preds = kmeans.predict(Xr)            # ndarray
            ids.extend(batch_ids)                  # flatten ids
            labels.extend(preds.tolist())          # flatten labels
            batch_features.clear(); batch_ids.clear()

    if batch_features:
        Xb = to_csr(batch_features)
        Xr = svd.transform(Xb)
        Xr_vector.extend(Xr)
        
        kmeans.partial_fit(Xr)
        preds = kmeans.predict(Xr)
        ids.extend(batch_ids)
        labels.extend(preds.tolist())

    pdf = pd.DataFrame({"統一編號": ids, "cluster": labels, "Xr_vector":Xr_vector})

    return pdf, svd, kmeans, Xr_vector


def save_(pdf, spark, kmeans, svd):

    # For storing dataframe with vector
    pdf["統一編號"] = pdf["統一編號"].astype(str)
    pdf["Xr_vector"] = pdf["Xr_vector"].apply(
        lambda v: [float(x) for x in (v.tolist() if isinstance(v, np.ndarray) else v)]
    )
    
    schema = StructType([
        StructField("統一編號", StringType(), False),
        StructField("cluster", IntegerType(), True),
        StructField("Xr_vector", ArrayType(DoubleType()), True),
    ])
    
    sdf = spark.createDataFrame(pdf.to_dict(orient="records"), schema=schema)
    sdf.write.format("delta").mode("overwrite").save(OUT)

    # For storing models
    s3 = boto3.client(
        "s3",
        endpoint_url = 'http://minio:9000',
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    # save locally
    joblib.dump(svd, "/tmp/svd.pkl")
    joblib.dump(kmeans, "/tmp/kmeans.pkl")
    
    # upload to MinIO
    s3.upload_file("/tmp/svd.pkl", "deltabucket", "models/sk_svd.pkl")
    s3.upload_file("/tmp/kmeans.pkl", "deltabucket", "models/sk_kmeans.pkl")

    

def main():
    from sparksession import spark_session
    s = None
    try:
        print('script start')
        s = spark_session()
        df = read_gold(s)
        pdf, svd, kmeans = fit_predict(df)
        save_(pdf, s, kmeans, svd)
    except Exception as e:
        print(f"Error in spark job: {e}")


if __name__ == '__main__':
    main()