# Talabat Martech & Analytics Platform (Full)

![Talabat](images/talabat.png) ![Braze](images/braze.png) ![BigQuery](images/bigquery.png) ![AWS S3](images/s3.png)

**Opinionated, deployable scaffold for retention, CRM, growth, product analytics, and activation (Braze + Connected Content).**

This repository provides a **production-ready framework** to manage Talabat’s lifecycle, growth, and product analytics while connecting to Braze campaigns and personalized content delivery. It combines **data ingestion, predictive analytics, connected content, and monitoring** into one cohesive stack.

---

## 🚀 Features

### 1️⃣ Data Ingestion & CRM Sync

* Pull Braze user data directly into **BigQuery** (or via S3 for large exports).
* Automate incremental updates for **millions of users**.
* Supports **connected content** JSON generation per user.

### 2️⃣ Analytics & DBT Models

* **Retention & Lifecycle:** D1/D7/D30 retention, silent vs active churn, cohort analysis.
* **Growth & Product:** MAU/DAU trends, spikes/dips, category switching, basket analysis.
* **Marketing:** Campaign variance, voucher elasticity, multi-channel impact, MTA/MMM prep.
* Built-in **DBT models** for analytics, monitoring, and feature engineering.

### 3️⃣ Predictive Analytics

* ML notebooks and pipelines for:

  * **Churn prediction**
  * **Customer LTV (PLTV) modeling**
  * **Promo response & personalization scoring**
* Direct integration with connected content for **1:1 campaigns**.

### 4️⃣ Activation & Connected Content

* Generate **per-user JSON content** for Braze campaigns.
* Supports AWS S3 + optional CloudFront for secure delivery.
* Works for push, email, and in-app messaging.

### 5️⃣ Orchestration & Monitoring

* **Airflow DAGs** for daily, weekly, and monthly jobs.
* Slack alerts for:

  * Data ingestion delays
  * Variance between Braze and BigQuery counts
  * Predictive model failures
* Retention, funnel, and campaign metrics tracked automatically.

### 6️⃣ Documentation & Deployment

* `docs/architecture.md`: Full architecture diagram.
* `docs/connected_content.md`: Connected Content setup.
* `.env.example` and `profiles.yml` for dbt.
* `requirements.txt` for Python dependencies.

---

## 📂 Project Structure

```
martech_retention_talabat_full/
├── airflow/              # DAGs & plugins
├── dbt/                  # Models, tests, profiles
├── notebooks/            # ML & analytics notebooks
├── scripts/              # Braze sync, connected content, model trainer, Slack alerts
├── docs/                 # Architecture, lifecycle, growth, connected content
├── images/               # Talabat, Braze, BigQuery, S3, workflow
├── .env.example
├── requirements.txt
└── README.md
```

---

## ⚡ Quick Start

1. **Clone repository**

```bash
git clone <repo_url>
cd martech_retention_talabat_full
```

2. **Set environment variables**

```bash
cp .env.example .env
# Fill in BRAZE_API_KEY, BIGQUERY_PROJECT, etc.
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Run Airflow DAGs**

* Daily retention & ingestion pipelines
* Slack alert notifications

5. **Run ML notebooks**

* Churn, LTV, promo elasticity models
* Generate connected content JSON for campaigns

6. **Deploy DBT models**

```bash
dbt run --profiles-dir dbt/
```

---

## 📈 Analytics & Monitoring

* **Retention & Lifecycle:** Track churn, retention, reactivation.
* **Growth & Product:** DAU/MAU trends, basket analysis, category switching.
* **Marketing Campaigns:** Compare Braze vs BigQuery counts, identify spikes/dips.
* **Predictive:** Churn scoring, LTV prediction, connected content personalization.

---

## 🔧 Opinionated Practices

* Use **direct Braze → BigQuery** ingestion for small-to-medium segments.
* Use **S3 staging** for very large exports (>1M users).
* All **predictive models feed directly into connected content generation**.
* Airflow handles orchestration, retries, and alerting.
* dbt for analytics, monitoring, and features ensures **version control & reproducibility**.

---

## 📚 Documentation

* See `docs/architecture.md` for full architecture diagram.
* See `docs/connected_content.md` for personalized content setup.

---

## ✅ Benefits

* **End-to-end**: From Braze ingestion to predictive scoring to campaign activation.
* **Scalable & reliable**: Handles millions of users, multiple verticals, and campaigns.
* **Actionable analytics**: Retention, churn, LTV, basket, and voucher insights.
* **Single-person ownership-ready**: Everything a one-person Martech role needs.
