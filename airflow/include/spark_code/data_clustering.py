import os
from sparksession import spark_session

from sklearn.decomposition import TruncatedSVD
from sklearn.cluster import MiniBatchKMeans
from scipy.sparse import csr_matrix
import numpy as np
import joblib, boto3
from pyspark.sql import Row

import json

from init_log import initlog
logger = initlog(__name__)


def read_gold(spark):
    global OUT

    GOLD = os.getenv("GOLD_PATH","s3a://deltabucket/gold/wholeCorp_delta")
    OUT = os.getenv("CLUSTER_PATH","s3a://deltabucket/gold/wholeCorp_clusters")

    return spark.read.format("delta").load(GOLD)


# For small data < 5M rows
def to_mysql(ids, labels, Xr_vector, spark, batch_size):
    
    rows = [
        Row(
            統一編號=i,
            cluster=c,
            vector=json.dumps(x)
        ) for i,c,x in zip(ids, labels, Xr_vector)
    ]
    sdf = spark.createDataFrame(rows)
    
    mysql_url = "jdbc:mysql://mysql_db_container:3306/whole_corp"
    mysql_table = "staging_clusters_vector"

    mysql_properties = {
        "user": "root",
        "password": "!QAZ2wsx",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    num_partitions = 8

    (sdf.write
        .format("jdbc")
        .option("url", mysql_url)
        .option("dbtable", mysql_table)
        .option("user", mysql_properties["user"])
        .option("password", mysql_properties["password"])
        .option("driver", mysql_properties["driver"])
        .option("batchsize", batch_size)
        .option("numPartitions", num_partitions)
        .mode("append")
        .save()
    )

    print(f"✅ Successfully wrote DataFrame to MySQL table '{mysql_table}'")
    
    
# def to_mysql_spark(ids, labels, Xr_vector, spark):
#     rows = [
#         Row(
#             統一編號=i,
#             cluster=c,
#             vector=json.dumps(x)
#         ) for i,c,x in zip(ids, labels, Xr_vector)
#     ]
        
#     sdf = spark.createDataFrame(rows)
    
#     mysql_url = "jdbc:mysql://mysql_db_container:3306/whole_corp"
#     mysql_table = "wholecorp_clusters_vector"

#     mysql_properties = {
#         "user": "root",
#         "password": "!QAZ2wsx",
#         "driver": "com.mysql.cj.jdbc.Driver"
#     }

#     num_partitions = 8
#     batch_size = 5000

#     (sdf.write
#         .format("jdbc")
#         .option("url", mysql_url)
#         .option("dbtable", mysql_table)
#         .option("user", mysql_properties["user"])
#         .option("password", mysql_properties["password"])
#         .option("driver", mysql_properties["driver"])
#         .option("batchsize", batch_size)
#         .option("numPartitions", num_partitions)
#         .mode("append")
#         .save()
#     )

#     print(f"✅ Successfully wrote DataFrame to MySQL table '{mysql_table}'")

#     # read_df = (spark.read
#     #            .format("jdbc")
#     #            .option("url", mysql_url)
#     #            .option("dbtable", mysql_table)
#     #            .option("user", mysql_properties["user"])
#     #            .option("password", mysql_properties["password"])
#     #            .option("driver", mysql_properties["driver"])
#     #            .load())
#     # read_df.show(5)


def staging_to_real():
    
    import mysql.connector

    conn = mysql.connector.connect(
        host="mysql_db_container",
        port=3306,
        user="root",
        password="!QAZ2wsx",
        database="whole_corp"
    )
    cursor = conn.cursor()

    upsert_sql = f"""
    INSERT INTO wholecorp_clusters (統一編號, cluster, vector)
    SELECT 統一編號, cluster, vector FROM staging_clusters_vector
    ON DUPLICATE KEY UPDATE
        cluster = VALUES(cluster),
        vector  = VALUES(vector);
    """
    cursor.execute(upsert_sql)
    conn.commit()
    cursor.close()
    conn.close()


def fit_predict(data_whole, s, sample_frac=0.005, sample_cap=5000, batch_size=50000):
    
    try:
        """
        Run SVD + MiniBatchKMeans on a manageable sample.
        Returns: Spark DataFrame [統一編號, cluster, Xr_vector]
        """
    
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
                              shape=(len(rows), num_features))
    
        # 1) collect a small sample for SVD training
        sample_rows = []
        for i, row in enumerate(data_whole.sample(False, sample_frac, seed=42).toLocalIterator()):
            sample_rows.append(row['features'])
            if i >= sample_cap:   # hard cap
                break
    
        X_sample = to_csr(sample_rows)
    
        svd = TruncatedSVD(n_components=100, random_state=42).fit(X_sample)
    
        # # # --- Stage 2: incremental clustering
        # kmeans = MiniBatchKMeans(
        #     n_clusters=15,
        #     random_state=42,
        #     batch_size=batch_size,
        #     verbose=1,
        #     n_init='auto'
        # )
    
        # batch_features, batch_ids = [], []
        
        # count = 0
        # # --- Pass 1: training only
        # for row in data_whole.select("統一編號", "features").toLocalIterator():
        #     batch_features.append(row["features"])
        #     batch_ids.append(row["統一編號"])
            
        #     if len(batch_features) >= batch_size:
        #         # print(f"Training batch {count}")
        #         Xb = to_csr(batch_features)
        #         Xr = svd.transform(Xb)
        
        #         kmeans.partial_fit(Xr)
        
        #         batch_features.clear(); batch_ids.clear()
        #         count += batch_size
        
        # # train on leftovers
        # if batch_features:
        #     Xb = to_csr(batch_features)
        #     Xr = svd.transform(Xb)
        #     kmeans.partial_fit(Xr)
    
        # # Save kmeans model after training
        # joblib.dump(kmeans, "/tmp/kmeans_model.pkl")
        # print("KMeans model saved to /tmp/kmeans_model.pkl")
    
        print(f'Loading kmean model')
        kmeans = joblib.load("/tmp/kmeans_model.pkl")
        
        # --- Pass 2: prediction with final centers
        batch_features, batch_ids = [], []  # reset before 2nd loop
        batch_count = 1
        for row in data_whole.select("統一編號", "features").toLocalIterator():
            batch_features.append(row["features"])
            batch_ids.append(row["統一編號"])
        
            if len(batch_features) >= batch_size:
                # print(f"Predicting batch {count}")
                Xb = to_csr(batch_features)
                Xr = svd.transform(Xb)
        
                preds = kmeans.predict(Xr)
        
                ids = batch_ids
                labels = preds.tolist()
                vectors = Xr.tolist()
    
                print(f"start inserting batch {batch_count}")
                to_mysql(ids, labels, vectors, s, batch_size)
        
                batch_features.clear(); batch_ids.clear()
                batch_count += 1
        
        # predict leftovers
        if batch_features:
            Xb = to_csr(batch_features)
            Xr = svd.transform(Xb)
            preds = kmeans.predict(Xr)
        
            ids = batch_ids
            labels = preds.tolist()
            vectors = Xr.tolist()
        
            to_mysql(ids, labels, vectors, s, batch_size)
    
        print(f"All batches are are inserted. Now moving data from staging to ")

        staging_to_real()
        
        return svd, kmeans
    except Exception as e:
        logger.error(f'error occured {e}', exc_info=True)


def save_(kmeans, svd):

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
    s = None
    try:
        print('script start')
        s = spark_session()
        df = read_gold(s)
        svd, kmeans = fit_predict(df)
        save_(kmeans, svd)
    except Exception as e:
        print(f"Error in spark job: {e}")


if __name__ == '__main__':
    main()