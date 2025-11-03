

def all_params(query, page_size=10):
	# from_record = (page_number - 1) * page_size

	# Initialize the query parameters
	query_param = {
		# 'from': from_record,  # Use the 'from' parameter for pagination
		"query": {
			"bool": {
				"must": [
					{"exists": {"field": "vector"}}
				]
			}
		},
		"size": page_size,
		"track_total_hits": True,
		"_source": ['paper_id', 'title', 'abstract', 'categories', 'version', 'version_created',
			  		 'updated', 'published', 'comments', 'cluster', 'vector']
	}
	
	def query_condition(sub_query):
		query_condition = {
			"bool": {
				"should": [
					{"match": {"title": {"query": sub_query, "analyzer": "english", "boost": 3}}},
					{"wildcard": {"title.keyword": f"{sub_query.lower()}*"}},
					{"match": {"abstract": {"query": sub_query, "analyzer": "english", "boost": 2}}},
					{"match": {"categories": {"query": sub_query, "analyzer": "english", "boost": 1}}},
				]
				,
			"minimum_should_match": 1
			}
		}
		query_param["query"]["bool"]["must"].append(query_condition)


	# Add query conditions for query_1 and query_2
	if isinstance(query, list):
		for sub_query in query:
				query_condition(sub_query)
	else:
		query_condition(query)

	
	# # Add conditions for filters
	# if min_date:
	#     query_param['query']['bool']['must'].append({"range": {"updated": {"gte": f'{min_date}-01-01', "format": "yyyy-MM-dd"}}})
	# if max_date:
	#     query_param['query']['bool']['must'].append({"range": {"updated": {"lte": f'{max_date}-12-31', "format": "yyyy-MM-dd"}}})
	
	# Apply pagination
	query_param["size"] = page_size
	
	return query_param


def search_company_params(name):
	return {
		"size": 1,
		"query": {
			"match": {
				"abstract": {"query": name}
			}
		},
	}

def recommend_params(vector, cluster):
    return {
        "size": 30,
        "query": {
            "script_score": {
                "query": {
					# "bool":{
					# 	"must": [{"exists": {"field": "vector"}}]
					# }
                    "term": { "cluster": str(cluster) }   # ensure string match
                },
                "script": {
                    "source": """
                        double cos = cosineSimilarity(params.qv, 'vector') + 1.0;
                        return cos;
                    """,
                    "params": {
                        "qv": vector   # must be len=59
                    }
                }
            }
        }
    }


def knn_params(vector, k=10, num_candidates=100):
    return {
        "knn": {
            "field": "vector",
            "query_vector": vector,
            "k": k,
            "num_candidates": num_candidates
        },
        "_source": [
            "paper_id", "title", "abstract", "categories",
            "version", "updated", "published", "comments"
        ]
    }



# def init_param():
# 	return  {
# 		"size": 0,  # We do not need to return documents
# 		"aggs": {
# 			"max_date": {
# 				"max": {
# 					"field": "updated"
# 				}
# 			},
# 			"min_date": {
# 				"min": {
# 					"field": "updated"
# 				}
# 			},
# 		}
# 	}