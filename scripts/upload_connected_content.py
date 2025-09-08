import os, json, hashlib
from google.cloud import bigquery
import boto3
from botocore.config import Config

BQ_PROJECT = os.getenv('BIGQUERY_PROJECT')
BQ_DATASET = os.getenv('DBT_DATASET','analytics')
TABLE = os.getenv('CONNECTED_TABLE','promo_content')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_PREFIX = os.getenv('S3_PREFIX','content/')
IMAGE_CDN_BASE = os.getenv('IMAGE_CDN_BASE','https://cdn.yourcompany.com/images/')
PUBLIC_CACHE_SECONDS = int(os.getenv('PUBLIC_CACHE_SECONDS','3600'))

session = boto3.session.Session()
s3 = session.client('s3', config=Config(s3={'addressing_style':'virtual'}))
from google.cloud import bigquery
bq = bigquery.Client()

def _payload(row):
    voucher = getattr(row, 'voucher_code', None)
    lang = getattr(row, 'language', 'en')
    return {
        'title': 'Special for you' if not lang.startswith('ar') else 'عرض خاص لك',
        'cta': 'Order now and save!',
        'voucher': voucher,
        'image_url': f"{IMAGE_CDN_BASE}{(voucher or 'DEFAULT')}.png"
    }

def _key(user_id):
    shard = hashlib.md5(str(user_id).encode()).hexdigest()[:2]
    return f"{S3_PREFIX}{shard}/user_{user_id}.json"

def upload_content():
    q = f"SELECT user_id, voucher_code, language FROM `{BQ_PROJECT}.{BQ_DATASET}.{TABLE}`"
    rows = bq.query(q).result()
    count = 0
    for r in rows:
        payload = json.dumps(_payload(r), ensure_ascii=False).encode('utf-8')
        key = _key(r.user_id)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=payload, ContentType='application/json', CacheControl=f'public, max-age={PUBLIC_CACHE_SECONDS}', ACL='public-read')
        count += 1
    print(f'Uploaded {count} files to s3://{S3_BUCKET}/{S3_PREFIX}')

if __name__ == '__main__':
    upload_content()
