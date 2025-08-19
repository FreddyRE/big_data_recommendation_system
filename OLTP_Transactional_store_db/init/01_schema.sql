-- Schema: OLTP for commerce app

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- USERS
CREATE TABLE IF NOT EXISTS app_user (
  user_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email        TEXT UNIQUE NOT NULL,
  full_name    TEXT,
  created_at   TIMESTAMPTZ DEFAULT now()
);

-- STORES
DROP TABLE IF EXISTS store CASCADE;

CREATE TABLE store (
  store_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name         TEXT NOT NULL,
  country_code CHAR(2),
  city         TEXT,
  lat          NUMERIC(9,6),
  lon          NUMERIC(9,6),
  created_at   TIMESTAMPTZ DEFAULT now(),
  CONSTRAINT store_unique_name_city UNIQUE (name, city)
);

-- ITEMS / CATALOG
CREATE TABLE IF NOT EXISTS item (
  item_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sku           TEXT UNIQUE,
  title         TEXT NOT NULL,
  category_path TEXT,   
  price_cents   INTEGER NOT NULL,
  active        BOOLEAN DEFAULT TRUE,
  updated_at    TIMESTAMPTZ DEFAULT now()
);

-- ORDERS (header)
CREATE TABLE IF NOT EXISTS "order" (
  order_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id      UUID REFERENCES app_user(user_id),
  store_id     UUID REFERENCES store(store_id),
  status       TEXT CHECK (status IN ('PLACED','PAID','SHIPPED','CANCELLED')),
  total_cents  INTEGER NOT NULL DEFAULT 0,
  created_at   TIMESTAMPTZ DEFAULT now()
);

-- ORDER LINES (details)
CREATE TABLE IF NOT EXISTS order_line (
  order_id     UUID REFERENCES "order"(order_id),
  line_no      INT,
  item_id      UUID REFERENCES item(item_id),
  qty          INT NOT NULL,
  price_cents  INT NOT NULL,
  PRIMARY KEY (order_id, line_no)
);