import httpx
import time
import logging

logger = logging.getLogger(__name__)


class VersatureClient:
    def __init__(self, *, access_token: str = None,
                 client_id: str = None, client_secret: str = None,
                 base_url: str = 'https://integrate.versature.com/api/',
                 api_version: str = 'application/vnd.integrate.v1.6.0+json'):
        self.base = base_url.rstrip('/') + '/'
        self.api_version = api_version
        self._token = access_token
        self._client_id = client_id
        self._client_secret = client_secret
        if not self._token and (self._client_id and self._client_secret):
            self._token = self._fetch_token()
        if not self._token:
            raise ValueError('Either access_token or client_id+client_secret required')
        self.headers = {
            'Authorization': f'Bearer {self._token}',
            'Accept': self.api_version,
        }

    def _fetch_token(self) -> str:
        logger.info('Fetching OAuth2 token via client_credentials')
        response = httpx.post(
            f'{self.base}oauth/token/',
            data={
                'grant_type': 'client_credentials',
                'client_id': self._client_id,
                'client_secret': self._client_secret,
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        logger.info('  Token obtained successfully')
        return data['access_token']

    def _get(self, path: str, params: dict = None):
        url = f'{self.base}{path}'
        logger.info(f'GET {url} params={params}')
        response = httpx.get(url, headers=self.headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict):
            logger.info(f'  -> status={response.status_code} keys={list(data.keys())}')
        else:
            logger.info(f'  -> status={response.status_code} type={type(data).__name__} len={len(data)}')
        return data

    def fetch_cdrs(self, start_date: str, end_date: str) -> list:
        """Fetch all CDRs for date range, handling cursor pagination.
        Versature returns {result: [...], cursor: ..., more: bool}."""
        all_records = []
        params = {'start_date': start_date, 'end_date': end_date}
        page = 0
        while True:
            page += 1
            data = self._get('cdrs/users/', params)
            # Versature uses 'result' key for CDR pagination
            records = data.get('result', data.get('data', data.get('results', [])))
            all_records.extend(records)
            logger.info(f'  CDR page {page}: {len(records)} records (total: {len(all_records)})')
            if not data.get('more', False):
                break
            params['cursor'] = data['cursor']
            time.sleep(1)
        return all_records

    def fetch_queue_stats(self, queue_id: str, start_date: str, end_date: str) -> dict:
        """Fetch queue stats. Versature needs business-hours time ranges.
        Returns either a dict or array[0]."""
        data = self._get(f'call_queues/{queue_id}/stats/', {
            'start_date': start_date, 'end_date': end_date
        })
        # Response may be a list (one stats object per day) or a dict
        if isinstance(data, list):
            return data[0] if data else {}
        return data.get('data', data) if isinstance(data, dict) else data

    def fetch_queue_splits(self, queue_id: str, start_date: str, end_date: str, period: str = 'day') -> list:
        """Fetch queue split reports. Versature returns a plain array."""
        data = self._get(f'call_queues/{queue_id}/reports/splits/', {
            'start_date': start_date, 'end_date': end_date, 'period': period
        })
        # Some endpoints return a plain array, others {data: [...]}
        if isinstance(data, list):
            return data
        return data.get('data', data.get('results', []))

    def fetch_queue_list(self) -> list:
        data = self._get('call_queues/')
        if isinstance(data, list):
            return data
        return data.get('data', data.get('results', []))
