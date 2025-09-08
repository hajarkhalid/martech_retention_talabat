import os, requests
from google.cloud import bigquery

SLACK_WEBHOOK = os.getenv('SLACK_WEBHOOK')

def post_slack(msg):
    if not SLACK_WEBHOOK:
        print('Slack webhook not configured:', msg)
        return
    try:
        requests.post(SLACK_WEBHOOK, json={'text': msg}, timeout=10)
    except Exception as e:
        print('Slack post failed', e)

def check_and_alert():
    # Basic freshness check for orders & events
    proj = os.getenv('BIGQUERY_PROJECT')
    ds = os.getenv('DBT_DATASET','analytics')
    client = bigquery.Client()
    q = f"SELECT 'orders' AS tbl, MAX(order_ts) AS last_ts FROM `{proj}.{ds}.orders` UNION ALL SELECT 'events', MAX(event_ts) FROM `{proj}.{ds}.events`"
    rows = list(client.query(q).result())
    msg = 'Data freshness:\n' + '\n'.join([f'{r.tbl}: {r.last_ts}' for r in rows])
    post_slack(msg)
