import os, joblib
from google.cloud import bigquery
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor

BQ_PROJECT = os.getenv('BIGQUERY_PROJECT')
BQ_DATASET = os.getenv('DBT_DATASET','analytics')

def load_table(table):
    client = bigquery.Client()
    return client.query(f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}` LIMIT 10000").to_dataframe()

def train_churn():
    df = load_table('features_user_basic')
    if 'churn_label' not in df.columns:
        raise RuntimeError('churn_label missing')
    X = df[['total_orders','avg_order_value']].fillna(0)
    y = df['churn_label'].astype(int)
    m = RandomForestClassifier(n_estimators=100, random_state=42)
    m.fit(X,y)
    joblib.dump(m,'/tmp/churn_model.joblib')
    print('churn model saved')

def train_ltv():
    df = load_table('features_user_orders')
    X = df[['orders_90d','revenue_90d']].fillna(0)
    y = df.get('ltv_label', df['revenue_90d'])  # fallback
    m = GradientBoostingRegressor(n_estimators=100, random_state=42)
    m.fit(X,y)
    joblib.dump(m,'/tmp/ltv_model.joblib')
    print('ltv model saved')

if __name__ == '__main__':
    train_churn()
    train_ltv()
