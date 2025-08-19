-- Seed data for OLTP commerce app
\set ON_ERROR_STOP on

BEGIN;

-- USERS
INSERT INTO app_user (email, full_name, created_at)
SELECT format('user%03s@example.com', g),
       format('User %s', g),
       now() - (random() * 60 || ' days')::interval
FROM generate_series(1,100) g
ON CONFLICT (email) DO NOTHING;

-- STORES
INSERT INTO store (name, country_code, city, lat, lon, created_at) VALUES
  ('Downtown ATL', 'US', 'Atlanta',   33.7490, -84.3880, now()),
  ('Miami Central','US', 'Miami',     25.7617, -80.1918, now()),
  ('NYC Midtown',  'US', 'New York',    40.7549,  -73.9840, now()),
  ('SF Market St', 'US', 'San Francisco',37.7749, -122.4194, now())
ON CONFLICT (name, city) DO NOTHING;

-- ITEMS
WITH cats AS (
  SELECT unnest(ARRAY[
    'Electronics>Phones>Android',
    'Electronics>Phones>iOS',
    'Home>Kitchen>Appliances',
    'Sports>Fitness>Accessories',
    'Books>Tech>Data'
  ]) AS category_path
)
INSERT INTO item (sku, title, category_path, price_cents, active, updated_at)
SELECT format('SKU-%05s', g),
       format('Item %s', g),
       (SELECT category_path FROM cats OFFSET (g % 5) LIMIT 1),
       (1000 + (random()*9000))::int,
       TRUE,
       now() - (random() * 30 || ' days')::interval
FROM generate_series(1,50) g
ON CONFLICT (sku) DO NOTHING;

-- ORDERS
WITH new_orders AS (
  INSERT INTO "order" (user_id, store_id, status, created_at)
  SELECT
    (SELECT user_id FROM app_user ORDER BY random() LIMIT 1),
    (SELECT store_id FROM store ORDER BY random() LIMIT 1),
    'PLACED',
    now() - (random() * 30 || ' days')::interval
  FROM generate_series(1,200)
  RETURNING order_id
)
-- ORDER LINES
INSERT INTO order_line (order_id, line_no, item_id, qty, price_cents)
SELECT o.order_id,
       ln.line_no,
       it.item_id,
       (1 + (random()*3)::int) AS qty,
       it.price_cents
FROM new_orders o
CROSS JOIN LATERAL (VALUES (1),(2),(3)) AS ln(line_no)
CROSS JOIN LATERAL (
  SELECT item_id, price_cents FROM item ORDER BY random() LIMIT 1
) it;

-- Backfill totals
UPDATE "order" o
SET total_cents = s.sum_cents
FROM (
  SELECT order_id, SUM(qty * price_cents) AS sum_cents
  FROM order_line
  GROUP BY order_id
) s
WHERE o.order_id = s.order_id;

COMMIT;