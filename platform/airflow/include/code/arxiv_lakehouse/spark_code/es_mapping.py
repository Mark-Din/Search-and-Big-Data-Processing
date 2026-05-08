'''-----------------------------Create mapping for user----------------------------------'''
def arxiv_mapping():
    # Define the settings for the custom analyzer
    body = {
        "settings":{
            "analysis": {
                "filter": {
            "my_stopwords": {
                "type": "stop",
                "stopwords":  [
                            "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
                              "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", 
                              "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", 
                              "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", 
                              "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", 
                              "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", 
                              "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", 
                              "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", 
                              "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", 
                              "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"
                            ]
                    }
                }
            }
        },        
      "mappings": {
        "properties": {
          "paper_id": { "type": "keyword" },
          "title":     { "type": "text", "analyzer": "english" },
          "abstract":  { "type": "text", "analyzer": "english" },
          "categories":{ "type": "keyword" },
          "version":   { "type": "keyword" },
          "version_created": { "type": "date" },
          "updated":   { "type": "date" },
          "published": { "type": "date" },
          "comments":  { "type": "text", "analyzer": "english" },
          "cluster":   { "type": "integer" },
          "vector": {
            "type": "dense_vector",
            "dims": 59,
            "index": True,
            "similarity": "cosine"
          }
        }
      }
  }
    
    return body
