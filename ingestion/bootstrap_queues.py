"""
Bootstrap script: discovers queue IDs from the Versature API
and populates the queue_config table in MotherDuck.

Usage:
    VERSATURE_TOKEN=... MOTHERDUCK_TOKEN=... python bootstrap_queues.py
"""
import duckdb
import os
import logging
from versature_client import VersatureClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    token = os.environ['VERSATURE_TOKEN']
    md_token = os.environ['MOTHERDUCK_TOKEN']

    client = VersatureClient(token)
    queues = client.fetch_queue_list()

    logger.info(f'Discovered {len(queues)} queues:')
    for q in queues:
        qid = q.get('id', q.get('queue_id'))
        desc = q.get('description', q.get('name', ''))
        logger.info(f'  {qid}: {desc}')

    conn = duckdb.connect(f'md:csh_analytics?motherduck_token={md_token}')

    for q in queues:
        qid = str(q.get('id', q.get('queue_id')))
        desc = q.get('description', q.get('name', ''))
        label = _infer_label(desc)
        conn.execute(
            "INSERT INTO queue_config (queue_id, queue_label, description) VALUES (?, ?, ?) "
            "ON CONFLICT (queue_id) DO UPDATE SET queue_label = ?, description = ?, discovered_at = current_timestamp",
            [qid, label, desc, label, desc]
        )
        logger.info(f'  Upserted queue {qid} -> label={label}')

    result = conn.sql('SELECT * FROM queue_config').fetchall()
    logger.info(f'queue_config now has {len(result)} rows')
    for r in result:
        logger.info(f'  {r}')

    conn.close()


def _infer_label(description: str) -> str:
    desc_lower = description.lower()
    if 'overflow' in desc_lower or 'ai' in desc_lower:
        return 'ai'
    elif 'french' in desc_lower or 'francais' in desc_lower:
        return 'french'
    elif 'english' in desc_lower:
        return 'english'
    return 'unknown'


if __name__ == '__main__':
    main()
