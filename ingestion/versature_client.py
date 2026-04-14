import httpx
import time
import logging

logger = logging.getLogger(__name__)


class VersatureClient:
    def __init__(self, access_token: str):
        self.base = 'https://integrate.versature.com/api/'
        self.headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/vnd.integrate.v1.6.0+json'
        }

    def _get(self, path: str, params: dict = None) -> dict:
        url = f'{self.base}{path}'
        logger.info(f'GET {url} params={params}')
        response = httpx.get(url, headers=self.headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f'  -> status={response.status_code} keys={list(data.keys()) if isinstance(data, dict) else "list"}')
        return data

    def fetch_cdrs(self, start_date: str, end_date: str) -> list:
        """Fetch all CDRs for date range, handling cursor pagination."""
        all_records = []
        params = {'start_date': start_date, 'end_date': end_date}
        page = 0
        while True:
            page += 1
            data = self._get('cdrs/users/', params)
            records = data.get('data', data.get('results', []))
            all_records.extend(records)
            logger.info(f'  CDR page {page}: {len(records)} records (total: {len(all_records)})')
            if not data.get('more', False):
                break
            params['cursor'] = data['cursor']
            time.sleep(1)
        return all_records

    def fetch_queue_stats(self, queue_id: str, start_date: str, end_date: str) -> dict:
        data = self._get(f'call_queues/{queue_id}/stats/', {
            'start_date': start_date, 'end_date': end_date
        })
        return data.get('data', data) if isinstance(data, dict) else data

    def fetch_queue_splits(self, queue_id: str, start_date: str, end_date: str, period: str = 'day') -> list:
        data = self._get(f'call_queues/{queue_id}/reports/splits/', {
            'start_date': start_date, 'end_date': end_date, 'period': period
        })
        return data.get('data', data.get('results', []))

    def fetch_queue_list(self) -> list:
        data = self._get('call_queues/')
        return data.get('data', data.get('results', []))
