CREATE DATABASE IF NOT EXISTS whole_corp;
USE whole_corp;

CREATE TABLE IF NOT EXISTS wholecorp_clusters_vector (
    統一編號 VARCHAR(10) PRIMARY KEY,
    cluster INT,
    vector JSON
);

CREATE TABLE IF NOT EXISTS staging_clusters_vector (
    統一編號 VARCHAR(20),
    cluster INT,
    vector JSON
);

