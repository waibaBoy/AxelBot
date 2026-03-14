import json
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier
import joblib

dataset_path = Path('data/feature_dataset.csv')
print(f'Using dataset: {dataset_path}')

df = pd.read_csv(dataset_path)
print('Rows:', len(df))

# Drop neutral labels for a cleaner binary setup.
df = df[df['label'] != 0].copy()

# Filter out fake "empty order book" states (spread > 50 cents)
df['market_spread'] = df['market_ask'] - df['market_bid']
df = df[df['market_spread'] < 0.50].copy()
print('Rows after non-neutral and illiquid filter:', len(df))

# Time-aware split to reduce leakage.
if 'ts' in df.columns:
    df['ts'] = pd.to_datetime(df['ts'], errors='coerce')
    df = df.sort_values('ts').reset_index(drop=True)

# Derive stationary features, drop absolute non-stationary prices
df['micro_price_dev'] = df['micro_price'] - df['mid']
df['fair_value_dev'] = df['fair_value'] - df['mid']

feature_cols = [
    'micro_price_dev', 'fair_value_dev', 'spread_bps', 'imbalance',
    'order_flow_signal', 'alpha_bps', 'inventory', 'news_bias_bps',
]

missing = [c for c in feature_cols if c not in df.columns]
if missing:
    raise ValueError(f'Missing feature columns: {missing}')

X = df[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
# Map {-1, +1} -> {0, 1}
y = (df['label'] > 0).astype(int)

split = int(len(df) * 0.8)
X_train, X_test = X.iloc[:split], X.iloc[split:]
y_train, y_test = y.iloc[:split], y.iloc[split:]
ret_test = df['fwd_ret_bps'].iloc[split:].values

print('Train:', len(X_train), '| Test:', len(X_test))

lr = Pipeline([
    ('scaler', StandardScaler()),
    ('clf', LogisticRegression(max_iter=2000, class_weight='balanced')),
])
lr.fit(X_train, y_train)
pred_lr = lr.predict(X_test)
proba_lr = lr.predict_proba(X_test)[:, 1]

print('\n--- Logistic Regression ---')
print(classification_report(y_test, pred_lr, digits=4))

xgb = XGBClassifier(
    n_estimators=300,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.9,
    colsample_bytree=0.9,
    objective='binary:logistic',
    eval_metric='logloss',
    tree_method='hist',
    random_state=42,
)
xgb.fit(X_train, y_train)
pred_xgb = xgb.predict(X_test)
proba_xgb = xgb.predict_proba(X_test)[:, 1]

print('\n--- XGBoost ---')
print(classification_report(y_test, pred_xgb, digits=4))

def pnl_proxy(prob, fwd_ret_bps, thr=0.55):
    # Long if prob > thr, short if prob < 1-thr, else flat.
    pos = np.where(prob > thr, 1.0, np.where(prob < (1.0 - thr), -1.0, 0.0))
    pnl = pos * fwd_ret_bps
    return {
        'active_ratio': float((pos != 0).mean()),
        'mean_bps': float(pnl.mean()),
        'sum_bps': float(pnl.sum()),
    }

print('\n--- PnL Proxies ---')
print('LR proxy:', pnl_proxy(proba_lr, ret_test, thr=0.55))
print('XGB proxy:', pnl_proxy(proba_xgb, ret_test, thr=0.55))

out_dir = Path('artifacts')
out_dir.mkdir(parents=True, exist_ok=True)

joblib.dump(lr, out_dir / 'alpha_logreg.joblib')
joblib.dump(xgb, out_dir / 'alpha_xgb.joblib')

meta = {
    'feature_columns': feature_cols,
    'label_definition': 'sign of forward return beyond threshold, mapped to binary up/down',
    'threshold_note': 'training notebook drops neutral labels (label==0)',
}
with open(out_dir / 'alpha_meta.json', 'w', encoding='utf-8') as f:
    json.dump(meta, f, indent=2)

print('\n--- Models Saved ---')
print('Saved:', out_dir / 'alpha_logreg.joblib')
print('Saved:', out_dir / 'alpha_xgb.joblib')
