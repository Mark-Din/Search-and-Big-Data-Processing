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

