# Imports and Configuration
from datetime import datetime
from typing import Optional
from fastapi import Query, Request, APIRouter, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse
import locale
from fastapi import Body
from pydantic import BaseModel

# Custom local imports
import sys
import os
# sys.path.append('/app/common')

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(os.path.join(PROJECT_ROOT, "search_system", "FastAPI_service", "code", "common"))
from init_log import initlog
from connection import ElasticSearchConnectionManager

logger = initlog('fastapi')
from queries.query_arxiv import all_params, knn_params
from queries.query_smb import all_params, recommend_params

# Configuration
locale.setlocale(locale.LC_ALL, 'zh_TW.UTF-8')
router = APIRouter(tags=["search"])

# Function to format a number as currency
def format_currency(number):
    formatted_amount = locale.format_string('%.f',float(number), grouping=True)
    return f'NT${formatted_amount}'


def tokenization(text, index_name) -> list:
    es = ElasticSearchConnectionManager._create_es_connection()
    """Tokenize a text string."""
    analysis_text = es.indices.analyze(index=index_name, body={"text": text, "analyzer": "english"})
    analyzed_tokens_list = [token['token'] for token in analysis_text['tokens']]
    return analyzed_tokens_list


# Elasticsearch Integration Functions
def search(index_name, search_param, es):
    """Perform a search query on Elasticsearch."""
    logger.info(f'index_name==========={index_name}')
    try:
        return es.search(index=index_name, body=search_param)
    except Exception as e:
        # es = ElasticSearchConnectionManager._create_es_connection()
        return es.search(index=index_name, body=search_param)

def search_query(es, index_name, search_params=None):
    """Query Elasticsearch and process the results."""
    results = []
    total_hits = 0  # Default total hits to 0

    if search_params is None:
        logger.warning('No search parameters provided')
        return results, total_hits  # Return empty results and 0 hits
    try:
        response = search(index_name, search_params, es)
        # logger.info('response: %s', response)

        if not response['hits']['hits']:
            logger.info('No results found')
            return results, total_hits  # Return empty results and 0 hits

        results_list = response['hits']['hits']
        for hit in results_list:
            hits = {key: hit['_source'][key] for key in hit['_source'] if key in hit['_source']}
            if '資本額' in hits:
                print("hits['資本額']:============", hits['資本額'])
                hits['資本額'] = format_currency(hits['資本額'])
            if '類別' in hits:
                hits['類別'] = hits['類別'].split(',')[0]
            if '設立日期' in hits:
                hits['設立日期'] = datetime.strptime(hits['設立日期'], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d") # '2024-03-22T00:00:00'
            results.append(hits)

        total_hits = response['hits']['total']['value']  # This line gets the total number of hits
    except Exception as e:
        logger.error(f"Error during search: {e}", exc_info=True)
        return results, total_hits  # Return whatever was gathered before the error

    return results, total_hits  


@router.get("/get_indices", response_class=JSONResponse)
async def perform_index_search(
        es: ElasticSearchConnectionManager = Depends(ElasticSearchConnectionManager.get_instance)
):
    try:
        indices = es.indices.get_alias(index="*,-.*")
        print('indices=================:', indices)
        logger.info(f'=========indices: {list(indices.keys())}=========')
        return JSONResponse(list(indices.keys()))
    except Exception as e:
        logger.error(f"Error fetching indices: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/full_search", response_class=HTMLResponse)
async def full_search(request: Request,
                        query: str = Query(...),
                        index_name: str = '',
                        es = Depends(ElasticSearchConnectionManager.get_instance)):
    
    # Tokenize the queries
    try:
        query = [q for q in tokenization(query, index_name) if q != 'undefined']

        logger.info(f'=========query: {query}=========')

        if query != '':
            search_params = all_params(query, page_size = 10)
            logger.debug(f'search_params:========={search_params}')
            results, total_hits = search_query(es, index_name, search_params)  # Assume search_query processes these params
            logger.debug(f'=========search_params: {search_params}===========')
            logger.debug('==================results: %s==================', results)
        else:
            results = []
            total_hits = 0

            if total_hits > 10000:
                total_hits = 10000
                
            logger.debug('=========total_hits: %s=========', total_hits)

        # Return the search results along with the request information
        return JSONResponse(content={"results": results, "total_hits": total_hits})
    except Exception as e:
        logger.error(f"Full search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/knn_search")
async def knn_search(
    vector: list = Body(..., embed=True),
    k: int = 10,
    es: ElasticSearchConnectionManager = Depends(ElasticSearchConnectionManager.get_instance)
):
    
    try:
        logger.info(f'KNN search with k={k} and vector length={len(vector)}')
        query = knn_params(vector, k)
        response = es.search(index=INDEX_NAME, body=query)
        results = [hit["_source"] for hit in response["hits"]["hits"]]
        return JSONResponse(content={"results": results, "total_hits": len(results)})
    except Exception as e:
        logger.error(f"KNN search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

