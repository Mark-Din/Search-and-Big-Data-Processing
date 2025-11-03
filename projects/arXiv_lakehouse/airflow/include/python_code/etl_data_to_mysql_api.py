import time
import feedparser
from datetime import date, datetime, timedelta

from include.init_log import initlog
from include.mysql_log import store_metadata
from include.connection import ElasticSearchConnectionManager as em

from include.category import category_map

logger = initlog(__name__)

CATEGORIES = list(category_map.keys())

# ============= TRANSFORM =============
def parse_authors(entry):
    logger.info(f"Parsing authors: {entry.authors}")
    return [a['name'] for a in entry['authors']] if entry.get("authors", None) else []

specific_date = date.today() - timedelta(days=1)
specific_date = date(2025,10,20)  # for testing
    
# ============= LOAD =============
conn = em.mysql_connection()
cursor = conn.cursor()

def main():
# ============= EXTRACT =============
    global COUNT
    COUNT = 0

    for cat in CATEGORIES:
        logger.info(f"Fetching category: {cat}")
        url = (
            "http://export.arxiv.org/api/query?"
            f"search_query=cat:{cat}&sortBy=lastUpdatedDate&sortOrder=descending&max_results=1000"
            # There is no date range selection, so if I want the oldest one, I can sert sortOrder=ascending
        )

        feed = feedparser.parse(url)

        today_papers = [
            entry for entry in feed.entries
            if datetime.strptime(entry.updated, "%Y-%m-%dT%H:%M:%SZ").date() > specific_date
        ]

        for entry in today_papers:

            paper_id_version = entry.id.split("/")[-1]
            paper_id = paper_id_version.split("v")[0]
            version = 'V' + paper_id_version.split("v")[1] if "v" in paper_id_version else "1"
            # 3️⃣ insert versions
            cursor.execute("SELECT 1 FROM papers WHERE id = %s", (paper_id,))
            if cursor.fetchone():
                cursor.execute("""
                    INSERT INTO versions (paper_id, version, created)
                    VALUES (%s, %s, %s)
                """, (paper_id, version, datetime.strptime(entry.updated, '%Y-%m-%dT%H:%M:%SZ')))
                COUNT += 1
        time.sleep(1)  # To respect arXiv's rate limits
        
    conn.commit()
    cursor.close()
    conn.close()

    logger.info("✅ ETL completed successfully!")


if __name__ == '__main__':

    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
    try:
        start = time.time()
        main()
        end = time.time()
        duration = end - start   # in seconds (float)
        status = 'S'
    except Exception as e:
        logger.error(f"ETL process failed {e}", exc_info=True)
        status = 'F'
    finally:
        try:
            store_metadata(
                run_id=run_id,
                stage_name='etl_api_to_mysql',
                component='python',
                record_count=COUNT if 'count' in locals() else 0,
                duration=duration if 'duration' in locals() else 0,
                status=status,
                note='ETL job from arXiv api to MySQL'
            )
        except Exception as e:
            logger.error(f"❌ Error storing metadata: {e}", exc_info=True)