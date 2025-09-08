import os, requests, json
from google.cloud import bigquery

BRAZE_ENDPOINT = os.getenv('BRAZE_REST_URL')
BRAZE_API_KEY = os.getenv('BRAZE_API_KEY')
SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK')
BQ_PROJECT = os.getenv('BIGQUERY_PROJECT')
BQ_DATASET = os.getenv('DBT_DATASET','analytics')

def query_bq(q):
    client = bigquery.Client()
    return list(client.query(q).result())

def trigger_braze_campaign_if_ready():
    # simple gating checks: ingestion delay and variance tables
    try:
        q = f"SELECT hours_delay FROM `{BQ_PROJECT}.{BQ_DATASET}.braze_ingestion_delay` LIMIT 1"
        rows = query_bq(q)
        if rows and rows[0].hours_delay > 2:
            post('Braze ingestion delayed; abort trigger')
            return
        q2 = f"SELECT variance_pct FROM `{BQ_PROJECT}.{BQ_DATASET}.braze_vs_bq_counts` LIMIT 1"
        rows2 = query_bq(q2)
        if rows2 and rows2[0].variance_pct > 5:
            post(f'Variance {rows2[0].variance_pct}% > threshold; abort trigger')
            return
        # trigger campaign
        campaign_id = os.getenv('BRAZE_CAMPAIGN_ID')
        if not campaign_id:
            post('BRAZE_CAMPAIGN_ID not set; skip')
            return
        url = f"{BRAZE_ENDPOINT.rstrip('/')}/campaigns/trigger/send"
        headers = {'Authorization': f'Bearer {BRAZE_API_KEY}', 'Content-Type': 'application/json'}
        payload = {'campaign_id': campaign_id, 'broadcast': True}
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        post(f'Braze campaign {campaign_id} triggered.')
    except Exception as e:
        post(f'Error triggering braze: {e}')

def post(msg):
    if SLACK_WEBHOOK:
        requests.post(SLACK_WEBHOOK, json={'text': msg})
    else:
        print(msg)
