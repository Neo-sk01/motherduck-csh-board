"""
Daily ingestion pipeline: pulls CDRs and queue data from Versature API
into MotherDuck csh_analytics tables.

Usage:
    VERSATURE_TOKEN=... MOTHERDUCK_TOKEN=... python run_daily.py [YYYY-MM-DD]
"""
import duckdb
import os
import sys
import logging
from datetime import date, timedelta
from versature_client import VersatureClient
from config import (
    TARGET_DNIS, ENGLISH_QUEUE_ID, FRENCH_QUEUE_ID,
    AI_OVERFLOW_EN_QUEUE_ID, AI_OVERFLOW_FR_QUEUE_ID,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def run(target_date: date = None):
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    start = str(target_date)
    end = str(target_date)
    batch_id = f'daily-{target_date}'

    token = os.environ['VERSATURE_TOKEN']
    md_token = os.environ['MOTHERDUCK_TOKEN']

    client = VersatureClient(token)
    conn = duckdb.connect(f'md:csh_analytics?motherduck_token={md_token}')

    logger.info(f'=== Ingestion for {target_date} ===')

    # --- Ingest CDRs ---
    ingest_cdrs(conn, client, start, end, target_date, batch_id)

    # --- Ingest queue stats + splits for each queue ---
    queues = {
        'english': ENGLISH_QUEUE_ID,
        'french': FRENCH_QUEUE_ID,
        'ai': AI_OVERFLOW_EN_QUEUE_ID,
    }
    # AI overflow FR is also labeled 'ai' for KPI purposes
    ai_queues = {
        AI_OVERFLOW_EN_QUEUE_ID: 'ai',
        AI_OVERFLOW_FR_QUEUE_ID: 'ai',
    }

    for label, queue_id in queues.items():
        ingest_queue_stats(conn, client, queue_id, label, start, end, target_date)
        ingest_queue_splits(conn, client, queue_id, label, start, end, target_date)

    # AI overflow FR — stats and splits with 'ai' label
    ingest_queue_stats(conn, client, AI_OVERFLOW_FR_QUEUE_ID, 'ai', start, end, target_date)
    ingest_queue_splits(conn, client, AI_OVERFLOW_FR_QUEUE_ID, 'ai', start, end, target_date)

    # --- Validate ---
    validate_day(conn, target_date)

    conn.close()
    logger.info(f'=== Ingestion complete for {target_date} ===')


def ingest_cdrs(conn, client, start, end, target_date, batch_id):
    logger.info(f'Fetching CDRs for {start} to {end}')
    cdrs = client.fetch_cdrs(start, end)
    logger.info(f'  Received {len(cdrs)} CDR records')

    if not cdrs:
        logger.warning('  No CDRs returned')
        return

    # Delete existing data for this date, then insert
    conn.execute(
        "DELETE FROM raw_cdrs WHERE CAST(start_time AS DATE) = ?",
        [target_date]
    )

    inserted = 0
    for r in cdrs:
        to_id = r.get('to', {}).get('id', '') if isinstance(r.get('to'), dict) else r.get('to_id', '')
        conn.execute(
            """INSERT INTO raw_cdrs (cdr_id, to_id, start_time, answer_time, end_time,
               duration, caller_name, user_extension, ingestion_batch)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                r.get('id'),
                to_id,
                r.get('start_time'),
                r.get('answer_time'),
                r.get('end_time'),
                r.get('duration', 0),
                r.get('caller_name', ''),
                r.get('user'),
                batch_id,
            ]
        )
        inserted += 1

    logger.info(f'  Inserted {inserted} CDRs into raw_cdrs')


def ingest_queue_stats(conn, client, queue_id, label, start, end, target_date):
    logger.info(f'Fetching queue stats for {label} (queue {queue_id})')
    stats = client.fetch_queue_stats(queue_id, start, end)
    logger.info(f'  Stats: offered={stats.get("calls_offered")}, '
                f'handled={stats.get("calls_handled")}, '
                f'abandoned={stats.get("abandoned_calls")}')

    conn.execute(
        "DELETE FROM raw_queue_stats WHERE queue_id = ? AND date_start = ?",
        [queue_id, target_date]
    )
    conn.execute(
        """INSERT INTO raw_queue_stats (queue_id, queue_label, date_start, date_end,
           calls_offered, calls_handled, abandoned_calls, abandoned_rate,
           average_talk_time, average_handle_time)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            queue_id, label, target_date, target_date,
            stats.get('calls_offered', 0),
            stats.get('calls_handled', 0),
            stats.get('abandoned_calls', 0),
            stats.get('abandoned_rate', 0),
            stats.get('average_talk_time', 0),
            stats.get('average_handle_time', 0),
        ]
    )
    logger.info(f'  Inserted queue stats for {label}')


def ingest_queue_splits(conn, client, queue_id, label, start, end, target_date):
    logger.info(f'Fetching queue splits for {label} (queue {queue_id})')
    splits = client.fetch_queue_splits(queue_id, start, end, 'day')
    logger.info(f'  Received {len(splits)} split intervals')

    for s in splits:
        interval_ts = s.get('interval', s.get('timestamp', str(target_date)))
        volume = s.get('volume', s.get('count', 0))
        conn.execute(
            """DELETE FROM raw_queue_splits
               WHERE queue_id = ? AND interval_ts = ? AND period_type = 'day'""",
            [queue_id, interval_ts]
        )
        conn.execute(
            """INSERT INTO raw_queue_splits (queue_id, queue_label, interval_ts, volume, period_type)
               VALUES (?, ?, ?, ?, 'day')""",
            [queue_id, label, interval_ts, volume]
        )

    logger.info(f'  Inserted {len(splits)} split records for {label}')


def validate_day(conn, target_date):
    """Run trust-addendum invariant checks and drift calculation."""
    logger.info(f'Validating {target_date}')

    row = conn.sql(
        f"SELECT * FROM v_kpi_daily WHERE call_date = '{target_date}'"
    ).fetchone()

    if row is None:
        logger.warning(f'  No KPI data for {target_date}')
        conn.execute(
            """INSERT OR REPLACE INTO validation_days
               (call_date, invariant_pass, drift_status, final_decision)
               VALUES (?, false, 'failed', 'fail')""",
            [target_date]
        )
        return

    # Column positions from v_kpi_daily:
    # 0: call_date, 1: kpi1_total_incoming, 2: kpi2_dropped_cdr,
    # 3: kpi2_dropped_queue, 4: kpi3_english, 5: kpi4_french, 6: kpi5_ai
    # ... 14: queue_offered_total, 15: reconciliation_drift_pct
    kpi1 = row[1]
    kpi2 = row[2]
    kpi3 = row[4]
    kpi4 = row[5]
    kpi5 = row[6]
    drift = row[15]

    # Hard-fail invariants (from Trust Addendum)
    invariant_pass = True
    reasons = []

    if (kpi3 + kpi4 + kpi5) > kpi1:
        invariant_pass = False
        reasons.append(f'queue_sum({kpi3}+{kpi4}+{kpi5}={kpi3+kpi4+kpi5}) > total_incoming({kpi1})')

    if kpi2 > kpi1:
        invariant_pass = False
        reasons.append(f'dropped({kpi2}) > total_incoming({kpi1})')

    # Drift check
    drift_status = 'clean'
    if not invariant_pass:
        drift_status = 'failed'
    elif drift is not None and drift > 2.0:
        drift_status = 'review_required'

    evidence = '; '.join(reasons) if reasons else None

    conn.execute(
        """INSERT OR REPLACE INTO validation_days
           (call_date, invariant_pass, drift_pct, drift_status, evidence)
           VALUES (?, ?, ?, ?, ?)""",
        [target_date, invariant_pass, drift or 0, drift_status, evidence]
    )

    # Update trust state if invariant failed
    if not invariant_pass:
        conn.execute(
            "UPDATE trust_state SET state = 'blocked', updated_at = current_timestamp WHERE id = 1"
        )
        logger.error(f'  BLOCKED: Invariant failure on {target_date}: {evidence}')
    elif drift_status == 'review_required':
        logger.warning(f'  REVIEW REQUIRED: {drift:.2f}% drift on {target_date}')
    else:
        logger.info(f'  CLEAN: {target_date} passed all checks (drift={drift or 0:.2f}%)')


if __name__ == '__main__':
    target = None
    if len(sys.argv) > 1:
        target = date.fromisoformat(sys.argv[1])
    run(target)
