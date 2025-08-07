# Imports and Configuration
from datetime import datetime
from fastapi import APIRouter, Depends, Query, status, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, Response
from elasticsearch.exceptions import ConnectionError
from pathlib import Path
import locale
from typing import Optional, List
from enum import Enum
from pydantic import BaseModel

# Custom local imports
import logging
from connection import ElasticSearchConnectionManager
from query import specific_dest_params, suggestion_params

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - [%(levelname)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)

# Set locale
locale.setlocale(locale.LC_ALL, 'zh_TW.UTF-8')

# Define paths
TEMPLATES_DIR = Path(__file__).parent / "templates"

# Initialize router
router = APIRouter(prefix='/search', tags=["search"])

# Function to format a number as currency
def format_currency(number):
    formatted_amount = locale.format_string('%.f',number, grouping=True)
    return f'NT${formatted_amount}'


def tokenization(text,es) -> list:
    """Tokenize a text string."""
    analysis_text = es.indices.analyze(index='lifecircleesg', body={"text": text, "analyzer": "traditional_chinese_analyzer"})
    # logger.info(f'=========analysis_text: {analysis_text}=========')
    analyzed_tokens_list = [token['token'] for token in analysis_text['tokens']]
    return analyzed_tokens_list


# Elasticsearch Integration Functions
def search(index_name, search_param):
    """Perform a search query on Elasticsearch."""
    try:
        return es.search(index=index_name, body=search_param)
    except Exception as e:
        es = ElasticSearchConnectionManager._create_es_connection()
        return es.search(index=index_name, body=search_param)


def search_query_suggestions(search_params=None, index_name=None, es=None):
    """Query Elasticsearch and process the results."""
    results = []

    if search_params is None:
        # logger.info('No search parameters provided')
        return results  # Return empty results and 0 hits
    try:
        response = search(index_name, search_params)
        # logger.info('============response========: %s', response)

        if not response['hits']['hits']:
            # logger.info('No results found')
            return results  # Return empty results and 0 hits

        results_list = response['hits']['hits']

        for hit in results_list:
            hits = {key: hit['_source'][key] for key in hit['_source'] if key in hit['_source']}
            hits['score'] = hit['_score']
            results.append(hits)

    except Exception as e:
        logger.error(f"Error during search: {e}")
        return results  # Return whatever was gathered before the error

    return results  


def search_query(search_params=None, index_name=None):
    """Query Elasticsearch and process the results."""
    results = []
    total_hits = 0  # Default total hits to 0

    if search_params is None:
        # logger.info('No search parameters provided')
        return results, total_hits  # Return empty results and 0 hits
    try:
        response = search(index_name, search_params)
        # # logger.info('response: %s', response)

        if not response['hits']['hits']:
            # logger.info('No results found')
            return results, total_hits  # Return empty results and 0 hits

        results_list = response['hits']['hits']
        for hit in results_list:
            hits = {key: hit['_source'][key] for key in hit['_source'] if key in hit['_source']}
            if 'updatedAt' in hits:
                    hits['updatedAt'] = datetime.strptime(hits['updatedAt'], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d") # '2024-03-22T00:00:00'
            if 'createdAt' in hits:
                hits['createdAt'] = datetime.strptime(hits['createdAt'], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d") # '2024-03-22T00:00:00'
            if 'price' in hits:
                hits['price'] = format_currency(hits['price'])
            hits['score'] = hit['_score']
            results.append(hits)

        total_hits = response['hits']['total']['value']  # This line gets the total number of hits
    except Exception as e:
        logger.error(f"Error during search: {e}")
        return results, total_hits  # Return whatever was gathered before the error

    return results, total_hits  


class ResultsType(str, Enum):
    courseesg = 'courseesg'
    useresg = 'useresg'
    lifecircleesg = 'lifecircleesg'
       

@router.get("/init", response_class=Response)
async def init_values():
    try:
        ElasticSearchConnectionManager.get_instance()
        return Response(status_code=status.HTTP_200_OK)
    except ConnectionError as e:
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=str(e))


@router.get("/search_suggestions", response_class=JSONResponse)
async def perform_search_suggestions(
    query: Optional[str] = Query(None,max_length=50), 
    es: ElasticSearchConnectionManager = Depends(ElasticSearchConnectionManager.get_instance)
    ):

    try:
        # Tokenize the queries
        query_tokens = tokenization(query,es)
        query_param_course, query_param_user, query_param_life, query_param_chat = suggestion_params(query_tokens)

        print(f'=========query_param_chat: {query_param_chat}=========')
        if query_tokens != []:
            results_course = search_query_suggestions(query_param_course, 'articleallgets', es)
            results_user = search_query_suggestions(query_param_user, 'useresg', es)
            results_life = search_query_suggestions(query_param_life, 'lifecircleesg', es)
            result_chat = search_query_suggestions(query_param_chat, 'chatesg', es)
        else:
            results_course = []
            results_user = []
            results_life = []
            result_chat = []
        
        return JSONResponse(content={"articleallgets": results_course, "useresg": results_user, "lifecircleesg": results_life, "chatesg": result_chat})
    except Exception as e:
        logger.error(f"Error during search: {e}")
        return JSONResponse(content={"articleallgets": [], "useresg": [], "lifecircleesg": [], "chatesg": []}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


# After clicking the search button, the search results are displayed on the search page
@router.get("/search_specific", response_class=HTMLResponse)
async def perform_search_specific(unique_id: Optional[str], ResultsType: ResultsType = Query(..., description="Choose a results type: courseesg, useresg, or lifecircleesg")):

    # Tokenize the queries
    query_param = specific_dest_params(unique_id, ResultsType)
    # logger.info(f'=========query_param_ResultsType: [{query_param}],[{ResultsType}]=========')

    results, total_hits = search_query(query_param, ResultsType)

    # Return the search results along with the request information
    return JSONResponse(content={"results": results})



# # Define Enums
# class PriceType(str, Enum):
#     all = None
#     paid = '付費'
#     free = '免費'
#     custom = '自行輸入'

# class VideoMode(str, Enum):
#     all = None
#     pic = '圖片'
#     one_video = '單一影片'
#     # multiple_video = '多章節影片'

# class VideoDuration(str, Enum):
#     all = None
#     short = '0-3小時'
#     medium = '3-6小時'
#     long = '6-12小時'
#     extra_long = '12小時以上'

# # Define Pydantic Model
# class SearchQuery(BaseModel):
#     query: Optional[str]
#     page_number: int = 1

# @router.post("/search_course", response_class=JSONResponse)
# async def perform_course_search(
#     SearchQuery: SearchQuery,
#     price_type: PriceType = Query(..., description="Choose a price type: 付費, 免費, or 自行輸入"),
#     price_range_small: Optional[int] = Query(None, description="Enter price range if 自行輸入 is selected"),
#     price_range_large: Optional[int] = Query(None, description="Enter price range if 自行輸入 is selected"),
#     videomode: VideoMode = Query(..., description="Choose a video mode: 圖片, 單一影片, 多章節影片(待加入)"),
#     videoduration: VideoDuration = Query(..., description="Choose a video duration"),
#     es: ElasticSearchConnectionManager = Depends(ElasticSearchConnectionManager.get_instance)
# ):
    
#     logger.info(f'=========SearchQuery: {SearchQuery}=========')
#     logger.info(f'=========price_type: {price_type}=========')
#     logger.info(f'=========price_range_small: {price_range_small}=========')
#     logger.info(f'=========price_range_large: {price_range_large}=========')
#     logger.info(f'=========videomode: {videomode}=========')
#     logger.info(f'=========videoduration: {videoduration}=========')

#     # Validate price range
#     if price_type == PriceType.custom and (not price_range_small or not price_range_large):
#         raise HTTPException(status_code=400, detail="Price range is required and should include two values when '自行輸入' is selected")
    
#     # Tokenize the queries
#     if SearchQuery.query != '':
#         query_tokens = tokenization(SearchQuery.query, es)
#     else:
#         query_tokens = []

#     if query_tokens:
#         search_params = course_params(query_tokens, price_type, price_range_small, price_range_large, videomode, videoduration, SearchQuery.page_number, page_size=8)
#         logger.info(f'=========search_params: {search_params}=========')
#         results, total_hits = search_query(search_params, index_name='course_sectionesg')
#     else:
#         results = []
#         total_hits = 0

#     if total_hits > 1000: total_hits = 1000

#     return JSONResponse(content={"results": results, "total_hits": total_hits})