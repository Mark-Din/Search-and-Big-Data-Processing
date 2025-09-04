'''-----------------------------Create mapping for user----------------------------------'''
def corp_mapping():
    # Define the settings for the custom analyzer
    body = {
        "settings":{
            "analysis": {
                "filter": {
                    "chinese_synonym":{
                        "type":"synonym",
                        "synonyms":[
                            "家具, 傢俱"
                        ]
                    },
                    "my_stopwords": {
                        "type": "stop",
                        "stopwords":  [
                                        "的", "了", "在", "是", "我", "有", "和", "就", "不", "人",
                                        "都", "一", "上", "也", "很", "到", "他", "年", "就是", "而",
                                        "我們", "這個", "可以", "這些", "自己", "沒有", "這樣", "著",
                                        "多", "對", "下", "但", "要", "被", "讓", "她", "向", "以",
                                        "所以", "把", "跟", "之", "其", "又", "在這裡", "這", "能",
                                        "應該", "則", "然後", "只是", "那", "在那裡", "這種", "因為",
                                        "這是", "而且", "如何", "誰", "它", "不是", "這裡", "如此",
                                        "每個", "這一點", "即使", "大", "小", "因此", "可能", "其他",
                                        "不過", "他們", "最後", "使用", "至於", "此", "其中", "大家",
                                        "或者", "最", "且", "雖然", "那麼", "這些", "一些", "通過",
                                        "為什麼", "什麼", "進行", "再", "已經", "不同", "整個", "以及",
                                        "從", "這樣的", "不能", "他的", "我們的", "自", "這邊", "那邊",
                                        "對於", "所有", "能夠", "請", "給", "在此", "上面", "以下",
                                        "儘管", "不需要", "不管", "與此同時", "關於", "有關", "將",
                                        "沒事", "沒關係", "這邊", "那邊", "有時候", "有時", "為", "可能性"
                                    ]
 
                    }
                },
                "analyzer": {
                    "traditional_chinese_analyzer": {
                        "type": "custom",
                        "tokenizer": "icu_tokenizer",
                        "filter": ["icu_folding", "my_stopwords"]
                    }
                }
            }
        },        
          "mappings": {
            "properties": {
              "統一編號": {
                "type": "keyword"
              },
              "公司名稱": {
                "type": "keyword"
              },
              "負責人": {
                "type": "keyword"
              },
              "登記地址": {
                "type": "keyword"
              },
              "資本額": {
                "type": "integer"
              },
              "營業項目及代碼表": {
                'type': 'text',
                'analyzer':'traditional_chinese_analyzer',
                "fields": {
                    "raw": { 
                        "type": "keyword",
                        "doc_values": True
                  }
              }
            },
              "縣市名稱": {
                "type": "text"
              },
              "類別_全": {
                "type": "text",
                'analyzer': "traditional_chinese_analyzer",
                "field" : {
                    "raw": {
                        "type": "keyword",
                        "doc_value": False
                    }
                }
              },
              "官網": {
                "type": "keyword",
              },
              "電話": {
                "type": "keyword",
              },
              "features": {
                  "type": "dense_vector",
                  "dims": 262147,  # ⚠️ match your vector size
                  "index": True,
                  "similarity": "cosine"
              }
    }
  }
}

    return body

