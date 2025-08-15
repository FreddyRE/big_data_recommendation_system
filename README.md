# Recommendation System Platform  
**Project: Big Data / Recommender Solution (Airflow + ML)**

> A full-stack **Big Data** recommendation platform that collects massive volumes of user events and batch data; processes them with distributed systems like **Hadoop (HDFS/YARN)** and **Apache Spark**; builds rich features; trains machine learning models; and serves results in real time.  
It supports content-based, collaborative, and hybrid strategies, and scales via Docker/Kubernetes (and Spark on YARN/K8s) to handle high throughput and large datasets.

---

## Table of Contents
- [What it is](#what-it-is)
- [Business Cases](#business-cases)
- [High-Level Architecture](#high-level-architecture)
- [Data Sources & Schemas](#data-sources--schemas)
- [Tech Stack](#tech-stack)
- [Local Quickstart](#local-quickstart)
- [Pipelines](#pipelines)
  - [Ingestion](#ingestion)
  - [Feature Engineering](#feature-engineering)
  - [Model Training & Registry](#model-training--registry)
  - [Real-Time Serving](#real-time-serving)
  - [Feedback Loop & A/B Testing](#feedback-loop--ab-testing)
- [API](#api)
- [Data Quality, Lineage & Monitoring](#data-quality-lineage--monitoring)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Security & Privacy](#security--privacy)
- [Roadmap](#roadmap)
- [License](#license)

---


## Business Cases
- **Home feed** — Personalized *Top picks for you*  
- **PDP** — *Similar items* and *Frequently bought together*  
- **Cart/Checkout** — Smart add-ons to lift AOV (Average Order Value)
- **Email/Push** — Weekly picks & reminders from browsing history

---

## High-Level Architecture
HIGH-LEVEL ARCHITECTURE

**(Bronze/Silver/Gold) pattern**

**CDC (Change Data Capture)**
- Near-real-time sync for orders, inventory, user profiles where freshness matters (seconds–minutes)
- Stream every row-level insert/update/delete from a database as events

<img width="1351" height="741" alt="reco-architecture drawio" src="https://github.com/user-attachments/assets/6f97ba7d-dbb5-483b-8a7a-9fa2eae762f7" />


### 1) Capture what users do (events)
-  apps (web/mobile/email) send **click**, **view**, **add-to-cart**, and **purchase** events to **Kafka**.  

### 2) Land the data safely, then clean it
- **Spark Structured Streaming** reads events from Kafka and writes them as-is to **Lakehouse – Bronze (raw zone)**.  
- Separately, **Ingestion (Batch/CDC)** brings tables from **Catalog DB**, **Orders DB**, and **Users/Profile** into **Bronze** too.  
- **Airflow** runs jobs that clean/validate/standardize **Bronze → Silver (cleaned) → Gold (curated)**.

### 3) Build features the models need
- **Feature Jobs (Spark)** read **Gold** and compute:
  - **Item features:** popularity, embeddings
  - **User features:** affinities, recency/frequency
- Results go two places:
  - **Redis (Online Features)** for super-fast lookups at request time  
  - **Vector Index (FAISS/ScaNN)** for “find similar items fast”

### 4) Serve recommendations in real time
- **FastAPI Recommendation API** handles live requests:
  - Pulls user/item features from **Redis**
  - Pulls similar items/candidates from **Vector Index**
  - Applies **Business Rules/Filters** (in-stock, price caps, diversity, etc.)
  - Scores and ranks with the **Ranker** (LightGBM/XGBoost/NN)
  - Returns **Top-K** items to the app—fast.

### 5) Learn from outcomes and improve
- The API logs **impressions/clicks** back to **Kafka**.  
- **Airflow** uses those logs + **Gold** data to retrain.  
- New models are tracked & versioned in **MLflow Model Registry**.  
- **Airflow** can promote a better model to production.




## Data Sources & Schemas

**Main sources**
- **Clickstream**: page views, clicks, add-to-cart, purchases  
- **Orders**: orders + order items  
- **Catalog**: attributes, categories, price/stock, text/images  
- **Users**: profiles & preferences

**Event schema**
```json
{
  "event_id": "uuid",
  "user_id": "u_123",
  "session_id": "s_456",
  "event_type": "view|click|add_to_cart|purchase",
  "product_id": "sku_789",
  "ts": "2025-08-15T08:30:00Z",
  "context": { "referrer": "home", "device": "mobile", "price": 29.99 }
}
```

**Orders**
```sql
orders(order_id, user_id, order_ts, total)
order_items(order_id, product_id, qty, price)
```

---

## Tech Stack
- **Airflow** — Orchestrate batch/stream jobs & retraining
- **Kafka** — Real-time event streams
- **Spark** — Batch + Structured Streaming for features
- **Lakehouse** — S3/GCS/ADLS with Parquet/Delta/Iceberg
- **Redis** — Low-latency online features/cache
- **FastAPI** — Real-time recommendation API
- **Docker + Kubernetes** — Packaging, scaling, canary deploys
- **Prometheus/Grafana** — DQ, lineage, monitoring
---


