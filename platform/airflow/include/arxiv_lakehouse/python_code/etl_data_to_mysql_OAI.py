import requests
from bs4 import BeautifulSoup
from datetime import date
import time

from include.init_log import initlog
from include.mysql_log import store_metadata
from include.connection import ElasticSearchConnectionManager as em

logger = initlog(__name__)

# adjust date range
# today = date.today()
# yesterday = today - timedelta(days=1)

# Set fixed range for testing
yesterday, today = date(2025,10,20), date(2025,10,26)

BASE_URL = (
    "https://export.arxiv.org/oai2?"
    f"verb=ListRecords&metadataPrefix=arXiv&from={yesterday}&until={today}"
)

# ============= DB CONNECTION =============
conn = em.mysql_connection()
cursor = conn.cursor()

# Create tables if not exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS papers (
    id VARCHAR(50) PRIMARY KEY,
    title TEXT,
    abstract LONGTEXT,
    updated DATE,
    published DATE,
    categories TEXT,
    submitter TEXT,
    comments TEXT,
    journal_ref TEXT,
    report_no TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS versions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    paper_id VARCHAR(50),
    version VARCHAR(10),
    created DATETIME,
    FOREIGN KEY (paper_id) REFERENCES papers(id)
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS authors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    paper_id VARCHAR(50),
    name VARCHAR(255),
    FOREIGN KEY (paper_id) REFERENCES papers(id)
);
""")

conn.commit()

len_paper = 0
len_authors = 0
len_version = 0

# ============= EXTRACT =============
def fetch_oai_records(base_url):
    """Yield all <record> elements from OAI-PMH endpoint (handles pagination)."""
    url = base_url
    while True:
        logger.info(f"Fetching: {url}")
        r = requests.get(url)
        soup = BeautifulSoup(r.content, "lxml-xml")
        records = soup.find_all("record")
        for rec in records:
            yield rec
        token = soup.find("resumptionToken")
        if not token or not token.text:
            break
        url = f"https://export.arxiv.org/oai2?verb=ListRecords&resumptionToken={token.text}"
        time.sleep(3)  # polite pause


def OAI_extraction():
    len_paper = 0
    len_authors = 0
    len_version = 0
    # ============= TRANSFORM + LOAD =============
    logger.info(f"Processing a new record... for date {yesterday}, {today}")
    count = 0
    for record in fetch_oai_records(BASE_URL):
        
        logger.info(f"Processing a new record... {count}")
        meta = record.find("metadata")
        if not meta:
            continue

        arx = meta.find("arXiv")
        if not arx:
            continue
        
        # Extract updated date from header
        header = record.find("header")
        updated = header.find("datestamp").text if header and header.find("datestamp") else None
        
        paper_id = arx.find("id").text if arx.find("id") else None
        title = arx.find("title").text if arx.find("title") else None
        abstract = arx.find("abstract").text if arx.find("abstract") else None
        categories = arx.find("categories").text if arx.find("categories") else None
        submitter = arx.find("submitter").text if arx.find("submitter") else None
        comments = arx.find("comments").text if arx.find("comments") else None
        journal_ref = arx.find("journal-ref").text if arx.find("journal-ref") else None
        report_no = arx.find("report-no").text if arx.find("report-no") else None
        created = arx.find("created").text if arx.find("created") else None
        published = created  # usually same as first version date

        # ============= INSERT INTO PAPERS =============
        cursor.execute("""
            INSERT INTO papers (id, title, abstract, updated, published, categories,
                                submitter, comments, journal_ref, report_no)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                title=VALUES(title),    
                abstract=VALUES(abstract),
                updated=VALUES(updated),
                categories=VALUES(categories),
                comments=VALUES(comments),
                journal_ref=VALUES(journal_ref),
                report_no=VALUES(report_no);
            """, (paper_id, title, abstract, updated, published, categories,
                submitter, comments, journal_ref, report_no))
        
        len_paper += 1

        # ============= INSERT AUTHORS =============
        authors = arx.find_all("author")
        for a in authors:
            name = a.text.strip()
            cursor.execute("""
                INSERT INTO authors (paper_id, name) VALUES (%s, %s)
            """, (paper_id, name))
            len_authors += 1

        # ============= INSERT VERSIONS =============
        versions = arx.find_all("version")
        for v in versions:
            version = v.get("version")
            created_v = v.find("date").text if v.find("date") else None
            cursor.execute("""
                INSERT INTO versions (paper_id, version, created)
                VALUES (%s,%s,%s)
            """, (paper_id, version, created_v))
            len_version += 1
        count += 1
    conn.commit()

    logger.info("✅ ETL completed successfully!")
    cursor.close()
    conn.close()

    return len_paper, len_authors, len_version


def main():
    
    start = time.time()

    run_id = f"run_{time.strftime('%Y%m%d_%H%M%S')}"
    try:
        len_paper, len_authors, len_version = OAI_extraction()

        logger.info("Inserting into metadata...")
        status = 'S'
    except Exception as e:
        logger.error(f"ETL process failed {e}", exc_info=True)
        status = 'F'
    finally:
        try:
            end = time.time()
            duration = end - start   # in seconds (float)

            for table in ['papers', 'authors', 'versions']:
                store_metadata(
                    run_id=run_id,
                    stage_name='etl_oai_to_mysql',
                    component=f'python',
                    record_count={'papers': len_paper, 'authors': len_authors, 'versions': len_version}[table] if 'len_paper' in locals() and 'len_authors' in locals() and 'len_version' in locals() else 0,
                    duration=duration if 'duration' in locals() else 0,
                    status=status,
                    note='ETL job from arXiv OAI to MySQL table : [{table}]'
                )
        except Exception as me:
            logger.error(f"❌ Error storing metadata: {me}", exc_info=True)
            raise

if __name__ == '__main__':

    main() 

    