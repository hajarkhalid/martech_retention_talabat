-- item co-occurrence at order level
WITH items AS (SELECT order_id, item_id FROM {{ ref('order_items') }})
SELECT a.item_id AS item_a, b.item_id AS item_b, COUNT(*) AS cnt
FROM items a JOIN items b USING(order_id)
WHERE a.item_id < b.item_id
GROUP BY 1,2
ORDER BY cnt DESC
LIMIT 200
