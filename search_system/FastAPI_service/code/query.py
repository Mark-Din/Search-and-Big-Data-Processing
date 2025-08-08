def suggestion_params(querys):

    query_param_course = {
        'size':12,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"match": {"title": {"query": "", "analyzer": "traditional_chinese_analyzer"}}},
                                {"wildcard": {"title": {"value": ""}}}
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    {
                        "bool": {
                            "should": [
                                {"term": {"sharingLevel": 2}},
                                {"term": {"sharingLevel": 3}}
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ],
                'must_not': [
                    {
                        'bool': {
                            'must': [
                                {'exists': {'field': 'disabled'}},
                                {'term': {'disabled': 1}}
                            ]
                        }
                    }
                ]
            }
        },
        "_source": ["id","title","coverPicture","nodebbTid"],
        "track_total_hits": True
      }
    
    # Disable the search for students
    query_param_user = {
        'size':12,
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {"term": {"sharingLevel": 2}},
                                {"term": {"sharingLevel": 3}}
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ],
                'must_not': [
                    {'terms': {'userType': [0, 2]}},
                    {
                        'bool': {
                            'must': [
                                {'exists': {'field': 'disabled'}},
                                {'term': {'disabled': 1}}
                            ]
                        }
                    }
                ]
            }
        },
        "_source": ["id", "name", 'avatarImage', "nodebbUid"],
        "track_total_hits": True
    }

    query_param_life = {
        'size':12,
       "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"match": {"name": {"query": "", "analyzer": "traditional_chinese_analyzer"}}},
                                {"wildcard": {"name": {"value": ""}}}
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    {"term": {"visibility": 1}}
                ],
                'must_not': [
                    {
                        'bool': {
                            'must': [
                                {'exists': {'field': 'deleted'}},
                                {'term': {'deleted': 1}}
                            ]
                        }
                    }
                ]
            }
        },
          "_source": ["id", "name", "forumImage"],
          "track_total_hits": True
          }
               
    
    if isinstance(querys, list):
        for query in querys:
            append_query_course  = [{"match": {"title": {"query": query, "analyzer": "traditional_chinese_analyzer"}}},
                            {"wildcard": {"title": {"value": f"*{query}*"}}}]
            query_param_course["query"]["bool"]["must"][0]["bool"]["should"].extend(append_query_course)
            append_query = [{"match": {"name": {"query": query, "analyzer": "traditional_chinese_analyzer"}}},
                            {"wildcard": {"name": {"value": f"*{query}*"}}}]
            query_param_life["query"]["bool"]["must"][0]["bool"]["should"].extend(append_query)
            query_param_user["query"]["bool"]["should"].extend(append_query)


    return query_param_course, query_param_user, query_param_life


def specific_dest_params(unique_id, ResultsType):
    
    query_param = {
            "size": 1,
            "query": {
                'term':{
                    'id':unique_id
                }
            },
            "_source": [],
            "track_total_hits": True
          }

    if ResultsType == 'courseesg':
       query_param["_source"] = ['id', 'title', 'advancedPlacement', 'advancedPlacementTid', 'price',
              'startDate', 'introduction', 'coverPicture', 'originPreVideo',
              'convertPreVideo', 'preVideoLength', 'videoReady', 'instructorIdentity',
              'nodebbUidTeacher', 'nodebbUidOwner', 'groupOwnerNodebbUid',
              'nodebbBigCid', 'nodebbSmallCid', 'nodebbTid', 'nodebbPid', 'disabled',
              'createdAt', 'updatedAt', 'preVideoReady', 'sumVideoLength',
              'authorizeStatus']
    elif ResultsType == 'useresg':
       query_param["_source"] = ['id', 'account', 'email', 'phone', 'nodebbUid', 'name', 'contactName',
       'userType', 'avatarImage', 'coverImage', 'experienceESG', 'experience',
       'education', 'personalIntroduce', 'companyName', 'uniformNumber',
       'jobTitle', 'companyAddress', 'companyTelephone',
       'companyTelephoneExtension', 'estimatedNumberOfPeople',
       'trainingObjective', 'level', 'disabled', 'createdAt', 'updatedAt']
    else:
        query_param["_source"] = ['id', 'name', 'nodebbCid', 'nodebbUid', 'visibility', 'about',
       'coverImage', 'forumImage', 'deleted', 'createdAt', 'updatedAt']
    return query_param


def mutiple_specific_param(query, page_number, page_size):
   
    return {
       "from": (page_number - 1) * page_size,
        "size": 10,
        "query": {
            "bool": {
                "should": [
                  {"match": {"title": {"query": query, "analyzer": "traditional_chinese_analyzer"}}},
                  {"match": {"introduction": {"query": query, "analyzer": "traditional_chinese_analyzer"}}},
                  {"wildcard": {"title": {"value": f"*{query}*"}}},
                  {"wildcard": {"introduction": {"value": f"*{query}*"}}}
                ]
            }
        },
        "_source": [
          'id', 'title', 'advancedPlacement', 'advancedPlacementTid', 'price',
          'startDate', 'introduction', 'coverPicture', 'originPreVideo',
          'convertPreVideo', 'preVideoLength', 'videoReady', 'instructorIdentity',
          'nodebbUidTeacher', 'nodebbUidOwner', 'groupOwnerNodebbUid',
          'nodebbBigCid', 'nodebbSmallCid', 'nodebbTid', 'nodebbPid', 'disabled',
          'createdAt', 'updatedAt', 'preVideoReady', 'sumVideoLength',
          'authorizeStatus'
          ],
        "track_total_hits": True
    }
    

