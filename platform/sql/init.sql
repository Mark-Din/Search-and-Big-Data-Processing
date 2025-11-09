CREATE DATABASE IF NOT EXISTS arxiv CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE arxiv;

CREATE TABLE IF NOT EXISTS coauthorship_edges (
    source VARCHAR(255),
    target VARCHAR(255),
    weight INT DEFAULT 1,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, target)
);

CREATE TABLE IF NOT EXISTS coauthor_stats (
    author VARCHAR(255) PRIMARY KEY,
    degree INT,
    degree_centrality FLOAT,
    betweenness FLOAT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);


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


