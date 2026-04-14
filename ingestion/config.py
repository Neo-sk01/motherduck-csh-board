import os

TARGET_DNIS = ['16135949199', '6135949199']
API_BASE = 'https://integrate.versature.com/api/'
ACCEPT_HEADER = 'application/vnd.integrate.v1.6.0+json'

# Queue IDs discovered from Versature API (via bootstrap_queues.py)
ENGLISH_QUEUE_ID = os.environ.get('QUEUE_ENGLISH', '8020')
FRENCH_QUEUE_ID = os.environ.get('QUEUE_FRENCH', '8021')
AI_OVERFLOW_EN_QUEUE_ID = os.environ.get('QUEUE_AI_OVERFLOW_EN', '8030')
AI_OVERFLOW_FR_QUEUE_ID = os.environ.get('QUEUE_AI_OVERFLOW_FR', '8031')
