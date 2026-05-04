import json
import os
import signal
import sys
import time
from typing import Any, Dict, Optional, List

import torch
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from sentence_transformers import SentenceTransformer

from init_log import initlog

logger = initlog(__name__)

# -----------------------------
# Config
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka_qidu:9092")
KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "postgres.public.files,postgres.public.logs").split(",")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "python-consumer")

# ES_HOST = os.getenv("ES_HOST", "http://elasticsearch_qidu:9200")
ES_HOST = os.getenv("ES_HOST", "http://10.11.60.43:9200")
ES_USERNAME = os.getenv("ES_USERNAME", "elastic")
ES_PASSWORD = os.getenv("ES_PASSWORD", "gAcstb8v-lFCVzCBC__a")

EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", "paraphrase-multilingual-mpnet-base-v2")
BULK_BATCH_SIZE = int(os.getenv("BULK_BATCH_SIZE", "100"))
BULK_FLUSH_INTERVAL_SEC = int(os.getenv("BULK_FLUSH_INTERVAL_SEC", "5"))
CONSUMER_POLL_TIMEOUT_MS = int(os.getenv("CONSUMER_POLL_TIMEOUT_MS", "1000"))

FILES_TOPIC = "postgres.public.files"
LOGS_TOPIC = "postgres.public.logs"

FILES_INDEX = "files"
QUERIES_INDEX = "queries"

# Graceful stop flag
RUNNING = True

# Small in-memory embedding cache
EMBEDDING_CACHE: Dict[str, List[float]] = {}


# -----------------------------
# Signal handling
# -----------------------------
def handle_shutdown(signum, frame):
    global RUNNING
    logger.info(f"Received shutdown signal: {signum}")
    RUNNING = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


# -----------------------------
# Model
# -----------------------------
device = "cuda" if torch.cuda.is_available() else "cpu"
logger.info(f"Loading embedding model on device={device}: {EMBEDDING_MODEL_NAME}")
model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device, local_files_only=True)


# -----------------------------
# Helpers
# -----------------------------
def safe_json_deserializer(m: Optional[bytes]) -> Optional[dict]:
    if not m:
        return None
    try:
        return json.loads(m.decode("utf-8"))
    except json.JSONDecodeError:
        logger.warning(f"Skipped non-JSON message: {m[:200]!r}")
        return None


def generate_embedding(text: Optional[str]) -> Optional[List[float]]:
    if not text:
        return None

    text = text.strip()
    if not text:
        return None

    if text in EMBEDDING_CACHE:
        return EMBEDDING_CACHE[text]

    with torch.no_grad():
        embedding = model.encode(text, normalize_embeddings=True)

    # Convert numpy array to plain list for JSON serialization / ES
    vector = embedding.tolist() if hasattr(embedding, "tolist") else list(embedding)
    EMBEDDING_CACHE[text] = vector
    return vector


def parse_json_field(raw_value: Any) -> Optional[dict]:
    if raw_value is None:
        return None
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str):
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON string field: {raw_value[:300]}")
            return None
    logger.warning(f"Unsupported JSON field type: {type(raw_value)}")
    return None


def get_index_name(topic: str) -> Optional[str]:
    if topic == FILES_TOPIC:
        return FILES_INDEX
    if topic == LOGS_TOPIC:
        return QUERIES_INDEX
    return None


def build_delete_action(topic: str, before_payload: dict) -> Optional[dict]:
    index_name = get_index_name(topic)
    if not index_name or not before_payload:
        return None

    if topic == LOGS_TOPIC:
        doc_id = before_payload.get("hash_id")
        if before_payload.get("action") != "ai_recommendation":
            return None
    elif topic == FILES_TOPIC:
        doc_id = before_payload.get("id")

    if doc_id is None:
        logger.warning("Delete event for files missing id")
        return None

    return {
        "_op_type": "delete",
        "_index": index_name,
        "_id": doc_id,
    }


def build_file_index_action(after: dict) -> Optional[dict]:
    doc_id = after.get("id")
    if doc_id is None:
        logger.warning("File event missing id")
        return None

    title = after.get("title") or ""
    content = after.get("content") or ""

    es_data = dict(after)
    es_data["content"] = content
    es_data["vector_title"] = generate_embedding(title)
    es_data["vector_content"] = generate_embedding(content)

    return {
        "_op_type": "index",   # use index so updates overwrite correctly
        "_index": FILES_INDEX,
        "_id": doc_id,
        "_source": es_data,
    }


def build_logs_index_action(after: dict) -> Optional[dict]:
    if after.get("action") != "ai_recommendation":
        return None

    detail = parse_json_field(after.get("details"))

    if not detail:
        logger.warning("Log event has invalid details JSON")
        return None
    
    user_id = after.get("user_id")
    doc_id = detail.get("hash_id")
    query_text = detail.get("segment_name", "")
    ml_response_raw = detail.get("ml_response")
    ai_recommendation = parse_json_field(ml_response_raw)

    es_data = {
        "id": doc_id,
        "owner_id": user_id,
        "query": query_text,
        "created_at": after.get("created_at"),
        "ai_recommendation": ai_recommendation,
        "accepted": False,
        "query_vector": generate_embedding(query_text),
    }

    return {
        "_op_type": "index",   # use index so repeated same hash updates safely
        "_index": QUERIES_INDEX,
        "_id": doc_id,
        "_source": es_data,
    }


def build_index_action(topic: str, after: dict) -> Optional[dict]:
    if topic == FILES_TOPIC:
        return build_file_index_action(after)
    if topic == LOGS_TOPIC:
        return build_logs_index_action(after)
    return None


def flush_actions(es: Elasticsearch, actions: List[dict]) -> bool:
    if not actions:
        return

    logger.info(f"Flushing {len(actions)} actions to Elasticsearch...")

    try:
        success, errors = helpers.bulk(
            es,
            actions,
            raise_on_error=False,
            stats_only=False,
            request_timeout=60,
        )

        if errors:
            for err in errors[:10]:
                logger.error(f"Bulk item error: {err}")
            logger.error(f"Bulk completed with {len(errors)} item errors")

        logger.info(f"Flushed bulk actions: success={success}, total={len(actions)}")
        return True
    except Exception as e:
        logger.error(f"Unexpected bulk flush error: {e}", exc_info=True)
        return False


# -----------------------------
# Main
# -----------------------------
def main():
    logger.info("Starting Kafka consumer...")

    try:
        consumer = KafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS], # The address of the Kafka broker(s)
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=safe_json_deserializer,
        )
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka: {e}", exc_info=True)
        sys.exit(1)

    try:
        es = Elasticsearch(
            [ES_HOST],
            basic_auth=(ES_USERNAME, ES_PASSWORD),
            request_timeout=30,
        )
        logger.info(f"Connected to Elasticsearch: {ES_HOST}")
    except Exception as e:
        logger.error(f"Failed to connect to Elasticsearch: {e}", exc_info=True)
        sys.exit(1)

    actions: List[dict] = []
    last_flush_time = time.time()

    logger.info(f"Consuming topics: {KAFKA_TOPICS}")

    try:
        while RUNNING:
            records = consumer.poll(timeout_ms=CONSUMER_POLL_TIMEOUT_MS)

            if not records:
                now = time.time()
                if actions and (now - last_flush_time >= BULK_FLUSH_INTERVAL_SEC):
                    success = flush_actions(es, actions)

                    if success:
                        consumer.commit()
                        actions.clear()
                        last_flush_time = now
                continue

            for _, messages in records.items():
                for message in messages:
                    value = message.value
                    if not value:
                        continue

                    topic = message.topic
                    payload = value.get("payload", {})
                    op = payload.get("op")
                    after = payload.get("after")
                    before = payload.get("before")

                    logger.info(f"Received topic={topic}, op={op}")

                    # DELETE
                    if op == "d":
                        delete_action = build_delete_action(topic, before or {})
                        if delete_action:
                            actions.append(delete_action)
                        continue

                    # Skip tombstone / malformed
                    if not after:
                        continue

                    action = build_index_action(topic, after)
                    if action:
                        actions.append(action)

                    if len(actions) >= BULK_BATCH_SIZE:
                        success = flush_actions(es, actions)
                        if success:
                            consumer.commit()
                            actions.clear()
                            last_flush_time = time.time()

            now = time.time()
            if actions and (now - last_flush_time >= BULK_FLUSH_INTERVAL_SEC): 
                success = flush_actions(es, actions)
                if success:
                    consumer.commit()
                    actions.clear()
                    last_flush_time = now

    except Exception as e:
        logger.error(f"Fatal error in consumer loop: {e}", exc_info=True)
    finally:
        if actions:
            logger.info("Flushing remaining actions before shutdown...")
            success = flush_actions(es, actions)
            if success:
                consumer.commit()
        try:
            consumer.close()
        except Exception:
            pass

        logger.info("Kafka consumer stopped cleanly.")


if __name__ == "__main__":
    main()