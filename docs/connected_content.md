# Connected Content (S3 + Braze)
1. dbt builds `promo_content` (user_id, voucher, content_url).
2. upload_connected_content.py writes per-user JSON to S3 under prefix like content/<shard>/user_<id>.json
3. Braze templates use Connected Content to fetch JSON at send time.
