# Imports and Configuration
import json
import re
from datetime import datetime
from typing import Optional
import numpy as np
import pandas as pd
from fastapi import FastAPI, Request, APIRouter, HTTPException, Depends, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import locale

from pydantic import BaseModel

# Custom local imports
from logs.logging_config import logger
from connection import ElasticSearchConnectionManager
from query import all_params, init_param, recommend_params

# Configuration
locale.setlocale(locale.LC_ALL, 'zh_TW.UTF-8')
TEMPLATES_DIR = Path(__file__).parent / "templates"
router = APIRouter(tags=["search"])
templates = Jinja2Templates(directory=TEMPLATES_DIR)

class CompanyData(BaseModel):
    公司名稱: str
    統一編號: str
    登記地址: str
    負責人_聯絡人: str
    設立日期: str
    資本額: str
    財政營業項目: str
    類別: str
    營業項目及代碼表: Optional[str] = None
    實收資本總額: Optional[str] = None
    attributes_vector: list


# Function to format a number as currency
def format_currency(number):
    formatted_amount = locale.format_string('%.f',number, grouping=True)
    return f'NT${formatted_amount}'


def tokenization(text) -> list:
    es = ElasticSearchConnectionManager._create_es_connection()
    """Tokenize a text string."""
    analysis_text = es.indices.analyze(index='smb_recommended', body={"text": text, "analyzer": "traditional_chinese_analyzer"})
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


def search_query(search_params=None):
    """Query Elasticsearch and process the results."""
    results = []
    total_hits = 0  # Default total hits to 0

    if search_params is None:
        logger.info('No search parameters provided')
        return results, total_hits  # Return empty results and 0 hits
    try:
        index_name = "smb_recommended"
        response = search(index_name, search_params)
        # logger.info('response: %s', response)

        if not response['hits']['hits']:
            logger.info('No results found')
            return results, total_hits  # Return empty results and 0 hits

        results_list = response['hits']['hits']
        for hit in results_list:
            hits = {key: hit['_source'][key] for key in hit['_source'] if key in hit['_source']}
            if '資本額' in hits:
                hits['資本額'] = format_currency(hits['資本額'])
            if '類別' in hits:
                hits['類別'] = hits['類別'].split(',')[0]
            if '設立日期' in hits:
                hits['設立日期'] = datetime.strptime(hits['設立日期'], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d") # '2024-03-22T00:00:00'
            results.append(hits)

        total_hits = response['hits']['total']['value']  # This line gets the total number of hits
    except Exception as e:
        logger.error(f"Error during search: {e}")
        return results, total_hits  # Return whatever was gathered before the error

    return results, total_hits  


# API Routes
@router.post("/init_values", response_class=JSONResponse)
async def set_search_params(request: Request):
    """Initialize search parameters."""
    search_params = init_param()
    response = search("smb_recommended", search_params)  

    # Extract min and max date from the aggregation results
    min_date = response['aggregations']['min_date']['value_as_string']  
    max_date = response['aggregations']['max_date']['value_as_string']
    min_capital = response['aggregations']['min_capital']['value']
    max_capital = response['aggregations']['max_capital']['value']
    
    return JSONResponse(content={
        "min_date": int(datetime.strptime(min_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y")),
        "max_date": int(datetime.strptime(max_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y")),
        "min_capital": min_capital,
        "max_capital": max_capital
    })


@router.post("/search", response_class=HTMLResponse)
async def perform_search(request: Request,
                        query_1: str = '',
                        query_2: str = '',
                        query_3: str = '',
                        location: str = '', 
                        min_date: str = None, 
                        max_date: str = None, 
                        min_capital: int = None, 
                        max_capital: int = None,
                        page_number: int = 1):
    
    # Tokenize the queries
    
    if query_1 != '': query_1 = [q for q in tokenization(query_1) if q != 'undefined']
    if query_2 != 'undefined': query_2 = [q for q in tokenization(query_2) if q != 'undefined']
    if query_3 != 'undefined': query_3 = [q for q in tokenization(query_3) if q != 'undefined']

    print(f'=========query_1: {query_1}=========')
    print(f'=========query_2: {query_2}=========')
    print(f'=========query_3: {query_3}=========')

    logger.info(f'=========query_after_tokenization: {query_1, query_2, query_3}=========')
    
    if query_1 != '':
        search_params = all_params(query_1, query_2, query_3, location, min_date, max_date, min_capital, max_capital, page_number , page_size = 10)
        results, total_hits = search_query(search_params)  # Assume search_query processes these params
        logger.info(f'=========search_params: {search_params}===========')
        logger.info('==================results: %s==================', results)
    else:
        results = []
        total_hits = 0

    if total_hits > 10000:
        total_hits = 10000
        
    logger.info('=========total_hits: %s=========', total_hits)
    logger.info('=========location: %s=========', location)

    # Return the search results along with the request information
    return JSONResponse(content={"results": results, "total_hits": total_hits})


@router.post("/recommend_search")
async def recommend_system(company_data: CompanyData, es: ElasticSearchConnectionManager = Depends(ElasticSearchConnectionManager.get_instance)):

    try:
        # Extract the search parameters
        search_params = recommend_params(company_data.類別, company_data.attributes_vector)
        results, total_hits = search_query(search_params)  # Assume search_query processes these params
        print(f'=========results&total_hits:{results} , {total_hits}=========')
        print(f'=========search_params: {search_params}===========')
        # Return extracted fields
        return JSONResponse(content={"results": results, "total_hits": total_hits})
    
    except AttributeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid data format: {e}")