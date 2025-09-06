def all_params(query_1, query_2, query_3, location, min_date, max_date, min_capital, max_capital, page_number, page_size=10):
    # print(f'=========query_1: {query_1}=========')
    # print(f'=========query_2: {query_2}=========')
    # print(f'=========query_3: {query_3}=========')
    from_record = (page_number - 1) * page_size

    # Initialize the query parameters
    query_param = {
        'from': from_record,  # Use the 'from' parameter for pagination
        "query": {
            "bool": {
                "must": []
            }
        },

        "size": page_size,
        "track_total_hits": True
    }
    
    # Add query conditions for query_1 and query_2
    for query in [query_1, query_2, query_3]:
        if query == "undefined" or not query:
            continue
        if isinstance(query, list):
          for sub_query in query:
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
        else:
          query_condition = {
              "bool": {
                  "should": [
                      {"term": {"公司名稱.keyword": {"value": query}}},
                      {"match": {"財政營業項目": {"query": query, "analyzer": "traditional_chinese_analyzer", "boost": 2}}},
                      {"match": {"類別": {"query": query, "analyzer": "traditional_chinese_analyzer", "boost": 3}}},
                      {"match": {"營業項目及代碼表": {"query": query, "analyzer": "traditional_chinese_analyzer"}}},
                      {"function_score": {"query": {"match": {"公司名稱": query}}, "boost": 5, "boost_mode": "multiply"}}
                  ],
                  "minimum_should_match": 1
              }
          }
          query_param["query"]["bool"]["must"].append(query_condition)
    
    # Add conditions for filters
    if min_date:
        query_param['query']['bool']['must'].append({"range": {"設立日期": {"gte": f'{min_date}-01-01', "format": "yyyy-MM-dd"}}})
    if max_date:
        query_param['query']['bool']['must'].append({"range": {"設立日期": {"lte": f'{max_date}-12-31', "format": "yyyy-MM-dd"}}})
    if min_capital:
        query_param['query']['bool']['must'].append({"range": {"資本額": {"gte": min_capital}}})
    if max_capital:
        query_param['query']['bool']['must'].append({"range": {"資本額": {"lte": max_capital}}})
    
    # Add location filter
    if location:
        # Split the location string and create match queries
        location_queries = [{"wildcard": {"登記地址": f'{loc.strip()}*'}} for loc in location.split(',')]
        # Add location queries as 'should' clauses under a 'bool' must condition
        if location_queries:
            query_param['query']['bool']['must'].append({"bool": {"should": location_queries}})
    
    # Apply pagination
    query_param["size"] = page_size
    

    return query_param



def recommend_params(type,vector):
    
    return {
          "size": 30, 
          "query": {
            "script_score": {
              "query": {
                "match": {
                  "類別": {"query": type}
                }
              },
              # Use the cosine similarity function to calculate the similarity between the query vector and the 'attributes_vector' field
              "script": {
                "source": """
                  double cosineSim = cosineSimilarity(params.query_vector, 'attributes_vector');
                  return sigmoid(1, Math.E, -cosineSim); 
                """,
                "params": {
                  "query_vector": vector
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