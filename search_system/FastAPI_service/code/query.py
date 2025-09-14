

def all_params(query, location, min_date, max_date, min_capital, max_capital, page_number, page_size=10):
	# print(f'=========query_1: {query_1}=========')
	# print(f'=========query_2: {query_2}=========')
	# print(f'=========query_3: {query_3}=========')
	from_record = (page_number - 1) * page_size

	# Initialize the query parameters
	query_param = {
		'from': from_record,  # Use the 'from' parameter for pagination
		"query": {
			"bool": {
				"must": [
					{"exists": {"field": "vector"}}
				]
			}
		},
		"size": page_size,
		"track_total_hits": True,
		"_source": ['統一編號', '公司名稱', '負責人', '登記地址', '資本額', '縣市名稱', '區域名稱',
       '縣市區域', '類別_全', '官網', '電話','cluster', 'vector']
	}
	
	def query_condition(sub_query):
		query_condition = {
			"bool": {
				"should": [
						{"term": {"公司名稱.keyword": {"value": sub_query}}},
						{"match": {"財政營業項目": {"query": sub_query, "analyzer": "traditional_chinese_analyzer", "boost": 2}}},
						{"match": {"類別": {"query": sub_query, "analyzer": "traditional_chinese_analyzer", "boost": 3}}},
						{"match": {"營業項目及代碼表": {"query": sub_query, "analyzer": "traditional_chinese_analyzer"}}},
						{"function_score": {"query": {"match": {"公司名稱": sub_query}}, "boost": 5, "boost_mode": "multiply"}}
			],
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
	#     query_param['query']['bool']['must'].append({"range": {"設立日期": {"gte": f'{min_date}-01-01', "format": "yyyy-MM-dd"}}})
	# if max_date:
	#     query_param['query']['bool']['must'].append({"range": {"設立日期": {"lte": f'{max_date}-12-31', "format": "yyyy-MM-dd"}}})
	# if min_capital:
	#     query_param['query']['bool']['must'].append({"range": {"資本額": {"gte": min_capital}}})
	# if max_capital:
	#     query_param['query']['bool']['must'].append({"range": {"資本額": {"lte": max_capital}}})
	
	# Add location filter
	# if location:
	#     # Split the location string and create match queries
	#     location_queries = [{"wildcard": {"登記地址": f'{loc.strip()}*'}} for loc in location.split(',')]
	#     # Add location queries as 'should' clauses under a 'bool' must condition
	#     if location_queries:
	#         query_param['query']['bool']['must'].append({"bool": {"should": location_queries}})
	
	# Apply pagination
	query_param["size"] = page_size
	

	return query_param



def search_company_params(name):
	return {
		"size": 1,
		"query": {
			"match": {
				"公司名稱": {"query": name, "analyzer": "traditional_chinese_analyzer"}
			}
		},
	}

def recommend_params(vector, cluster):
    return {
        "size": 30,
        "query": {
            "script_score": {
                "query": {
                    "term": { "cluster": str(cluster) }   # ensure string match
                },
                "script": {
                    "source": """
                        double cos = cosineSimilarity(params.qv, 'vector') + 1.0;
                        return cos;
                    """,
                    "params": {
                        "qv": vector   # must be len=100
                    }
                }
            }
        }
    }



def init_param():
	return  {
		"size": 0,  # We do not need to return documents
		"aggs": {
			"max_date": {
				"max": {
					"field": "設立日期"
				}
			},
			"min_date": {
				"min": {
					"field": "設立日期"
				}
			},
			"max_capital": {
				"max": {
					"field": "資本額"
				}
			},
			"min_capital": {
				"min": {
					"field": "資本額"
				}
			},
		}
	}