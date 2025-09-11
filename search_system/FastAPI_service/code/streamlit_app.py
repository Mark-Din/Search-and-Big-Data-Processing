import asyncio
import cProfile
import pstats
import re
import pandas as pd
import streamlit as st
import requests
from common import initlog  # Custom logger
from performance_test import profile_function
import httpx  # httpx is a fully featured HTTP client for Python 3, which provides sync and async APIs, and support for both HTTP/1.1 and HTTP/2.
logger = initlog('search_interface')
import warnings
from text_analyzing import TextAnalyzer

warnings.filterwarnings("ignore", category=UserWarning)

@st.cache_data
def get_indices():
    response = requests.get("http://127.0.0.1:8000/get_indices/")
    response.raise_for_status()  # Raise an error for HTTP error responses
    return response.json()

async def search_for_each_index(client, index, query):
    # Request to search within the selected index
    response = await client.get("http://127.0.0.1:8000/keyword_search/", params={"index_name": index, 'query': query})
    response.raise_for_status()  # Raise an error for HTTP error responses
    return response.json()

async def main(query):
    if not query:
        st.warning("Please enter a search query.")
        return

    # Initialize session state variables if they don't exist
    if 'query' not in st.session_state or query != st.session_state['query']:
        st.session_state['query'] = query
        st.session_state['results'] = {}
        st.session_state['search'] = True

        try:
            indices = get_indices()

            # Regular expression to match strings that do not contain the word "log"
            pattern = re.compile(r'^(?!.*log)')
            # Filter the list
            indices = [i for i in indices if pattern.match(i) and not i.startswith('.') and not i.startswith('_')]

            async with httpx.AsyncClient(timeout=60) as client:  # Reuse this client
                tasks = [search_for_each_index(client, index, query) for index in indices]  # Pass the correct index name here
                results = await asyncio.gather(*tasks)
                for index, result in zip(indices, results):
                    if result and result['total_hits'] > 0:
                        total_hits = result['total_hits']
                        st.write(f"Results for index: {index}, total_hits: {total_hits}")
                        st.write(pd.DataFrame(result['results']))
                        st.session_state['results'][index] = {'total_hits': total_hits, 'data': result['results']}

        except requests.exceptions.RequestException as e:
            st.error(f"HTTP Request failed: {e}") 
    else:
        if st.session_state['results']:
            for index, result in st.session_state['results'].items():
                if result['total_hits'] > 0:
                    st.write(f"Results for index: {index}, total_hits: {result['total_hits']}")
                    st.write(pd.DataFrame(result['data']))



def init_sidebar():
    # Initialize a session state variable that tracks the sidebar state (either 'expanded' or 'collapsed').
    if 'sidebar_state' not in st.session_state:
        st.session_state.sidebar_state = 'expanded'

        # Streamlit set_page_config method has a 'initial_sidebar_state' argument that controls sidebar state.
        st.set_page_config(initial_sidebar_state=st.session_state.sidebar_state)

        st.session_state['sidebar_state'] = 'expanded'
    # Show title and description of the app.

    # Add a button to toggle the sidebar state.
    if st.session_state.sidebar_state == 'expanded':
        if st.sidebar.button('Collapse sidebar', on_click=lambda: st.session_state.update(sidebar_state='collapsed')):
            st.session_state.sidebar_state = 'collapsed'
    else:
        if st.button('Expand sidebar', on_click = lambda: st.session_state.update(sidebar_state='expanded')):
            st.session_state.sidebar_state = 'expanded'
    

if __name__ == '__main__':

    init_sidebar()

    with cProfile.Profile() as profiler:

        # Set the title of the application
        st.title("Overall Search Interface")

        # Initialize session state variables if they don't exist
        if 'index' not in st.session_state:
            st.session_state['index'] = ''
        if 'search' not in st.session_state:
            st.session_state['search'] = False
        if 'query' not in st.session_state:
            st.session_state['query'] = ''
        if 'results' not in st.session_state:
            st.session_state['results'] = {}
            
        query = st.text_input('Enter search query').strip().replace(' ', '')

        asyncio.run(main(query))

        # Create a stats object and print the results
        stats = pstats.Stats(profiler)

        # Extract the total number of calls and the total time
        total_calls = stats.total_calls
        total_time = stats.total_tt
        # Log the summary information
        logger.info(f'=========Profiling Summary=========')
        logger.info(f'Total function calls: {total_calls}')
        logger.info(f'Total time taken: {total_time:.3f} seconds')

