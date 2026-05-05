import asyncio
import cProfile
import pstats
import pandas as pd
import streamlit as st
import requests
# from common import initlog  # Custom logger

import sys
sys.path.append('/app/common')
from init_log import initlog

import httpx  # httpx is a fully featured HTTP client for Python 3, which provides sync and async APIs, and support for both HTTP/1.1 and HTTP/2.
logger = initlog('search_interface')
import warnings

warnings.filterwarnings("ignore", category=UserWarning)


@st.cache_data
def get_indices():
    response = requests.get("http://127.0.0.1:3002/get_indices/")
    response.raise_for_status()  # Raise an error for HTTP error responses
    return response.json()

async def search_for_each_index(client, query):
    logger.info(f'query111:====, [{query}]')
    # Request to search within the selected index
    response = await client.post(
                                "http://127.0.0.1:3002/full_search",
                                params={'query': query, 'index_name': TABLE.lower()}
                            )
    if response.status_code != 200:
        print("Error:", response.status_code, response.text)
    response.raise_for_status()  # Raise an error for HTTP error responses
    return response.json()


async def search_for_similarity(client, name):
    logger.info(f'company_for_similarity: {name}')
    # Request to search within the selected index
    response = await client.get(
                                "http://127.0.0.1:3002/recommend_search",
                                params={'companyName': name}
                            )
    if response.status_code != 200:
        print("Error:", response.status_code, response.text)
    response.raise_for_status()  # Raise an error for HTTP error responses
    
    return response.json()


async def get_recommendations(vector):
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "http://127.0.0.1:3002/knn_search",
            json={"vector": vector, "k": 5, 'index_name': TABLE.lower()}
        )
        logger.info(f'Recommendation response status: {resp.json()}')
        return resp.json()


async def get_full_search_data(indices, results):
    """
    Display search results in Streamlit, with expandable details and optional recommendations.
    """
    # Ensure indices is a list
    if isinstance(indices, str):
        indices = [indices]

    for index, result in zip(indices, results):
        if not result or result.get("total_hits", 0) == 0:
            continue

        total_hits = result["total_hits"]
        df = pd.DataFrame(result["results"])

        st.subheader(f"Results for index: {index} — {total_hits} matches")
        logger.info(f'DataFrame columns: {df.columns.tolist()}')    

        # Display expandable results
        for i, row in df.iterrows():
            with st.expander(f"{row.get('title', '(Untitled)')}"):
                # Show all fields
                for col, val in row.items():
                    # Exclude vector and clusters from display
                    if col not in ['vector', 'cluster']:
                        st.write(f"**{col}**: {val}")

                # Auto-load recommended list
                rec_data = await get_recommendations(row["vector"])
                recs = pd.DataFrame(rec_data["results"])
                # Remove the current paper from recommendations if present
                recs = recs[recs['paper_id'] != row['paper_id']]
                st.markdown("#### 🔁 Recommended Similar Papers")
                st.dataframe(recs[["title", "categories", "published"]].head(5))


async def full_search(query):
    logger.info(f'query:====, {query}')
    if not query:
        st.warning("Please enter a search query.")
        return [], []   # return empty lists instead of None

    # Initialize session state variables if they don't exist
    if 'query' not in st.session_state or query != st.session_state['query']:
        st.session_state['query'] = query
        st.session_state['results'] = {}
        st.session_state['search'] = True

        try:
            # indices = get_indices()

            # Regular expression to match strings that do not contain the word "log"
            # pattern = re.compile(r'^(?!.*log)')
            # Filter the list
            # indices = [i for i in indices if pattern.match(i) and not i.startswith('.') and not i.startswith('_')]
            # logger.info(f'indices:====, {indices}')

            async with httpx.AsyncClient(timeout=60) as client:  # Reuse this client
                tasks = search_for_each_index(client, query)   # Pass the correct index name here
                results = await asyncio.gather(tasks)
                
                # logger.info(f'indices: {indices}, results: {results}')
                return 'whole_corp', results

        except requests.exceptions.RequestException as e:
            st.error(f"HTTP Request failed: {e}") 
            return [], []   # make sure to return something
    else:
        if st.session_state['results']:
            await get_full_search_data(st.session_state['results'].keys(), st.session_state['results'].values())

    return [], []   # default safe return


async def sim_for_company():

    company_for_sim = st.text_input('Try to find similarity? Enter company').strip()
    if not company_for_sim:
        st.warning("Please enter a company name for similarity search.")
        return [], []
    logger.info(f'company_for_sim:====, {company_for_sim}')
    async with httpx.AsyncClient(timeout=60) as client:  # Reuse this client
        result = await search_for_similarity(client, company_for_sim)
        logger.info(f'similarity result: {result}')
    return result, company_for_sim


async def main(query):

    indices, results = await full_search(query)
    
    await get_full_search_data(indices, results)

    # For similarity search
    if 'last_query' in st.session_state and st.session_state['last_query'] != "":
        result, company_for_sim = await sim_for_company()
        if result and result['total_hits'] > 0:
            total_hits = result['total_hits']
            st.write(f"Similarity search results for company: {company_for_sim}, total_hits: {total_hits}")
            st.write(pd.DataFrame(result['results']))
            st.session_state['results']['similarity_search'] = {'total_hits': total_hits, 'data': result['results']}
        else:
            st.write(f'No data found')

            
def init_sidebar():
    # Initialize a session state variable that tracks the sidebar state (either 'expanded' or 'collapsed').
    if 'sidebar_state' not in st.session_state:
        st.session_state.sidebar_state = 'expanded'

        # Streamlit set_page_config method has a 'initial_sidebar_state' argument that controls sidebar state.
        st.set_page_config(initial_sidebar_state=st.session_state.sidebar_state)

        st.session_state['sidebar_state'] = 'expanded'
    # Add a button to toggle the sidebar state.
    if st.session_state.sidebar_state == 'expanded':
        if st.sidebar.button('Collapse sidebar', on_click=lambda: st.session_state.update(sidebar_state='collapsed')):
            st.session_state.sidebar_state = 'collapsed'
    else:
        if st.button('Expand sidebar', on_click = lambda: st.session_state.update(sidebar_state='expanded')):
            st.session_state.sidebar_state = 'expanded'
    

if __name__ == '__main__':
    
    global TABLE
    TABLE = 'whole_corp'
    try:
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
                
            selected = st.selectbox('Select Table', ['arXiv', 'SMB'])

            if selected == 'arXiv':
                TABLE = 'arxiv_clusters'
            else:
                TABLE = "wholecorp_clusters_vector"

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
    except Exception as e:
        logger.error(f'Streamlit process failed {e}', exc_info=True)