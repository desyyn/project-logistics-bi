#D:\SEMESTER 5\Kecerdasan Bisnis\UAS\project-logistics\3_etl_scripts\T5_training_xgb.py
import pandas as pd
import numpy as np
import logging
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, confusion_matrix, mean_squared_error
from xgboost import XGBClassifier
import warnings, joblib
from collections import Counter
import sys

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INPUT_FILE = "/tmp/ml_dataset_ready.csv"
TARGET_COLUMN = "is_delayed"
OUTPUT_REPORT = "/tmp/xgb_model_report.txt"
OUTPUT_MODEL = "/tmp/xgb_otd_model.joblib"

try:
    df = pd.read_csv(INPUT_FILE)
    logger.info(f"✅ Loaded dataset: {len(df)} rows, {len(df.columns)} columns")
except Exception as e:
    logger.error(f"❌ ERROR loading dataset: {e}")
    sys.exit(1)

top_features = [
    "shipping_mode_Standard Class",
    "profit_per_order",
    "profit_per_quantity",
    "order_item_profit_ratio",
    "order_day",
    "effective_discount",
    "shipping_mode_Second Class",
    "order_item_discount_rate",
    "order_month",
    "sales"
]

missing_features = [f for f in top_features if f not in df.columns]
if missing_features:
    logger.error(f"❌ ERROR: Missing features in dataset: {missing_features}. Please check T4 encoding.")
    sys.exit(1)

df["profit_over_quantity"] = df["profit_per_order"] / (df["profit_per_quantity"] + 1e-5)
df["discount_ratio"] = df["effective_discount"] / (df["order_item_discount_rate"] + 1e-5)

feature_cols = top_features + ["profit_over_quantity", "discount_ratio"]
final_feature_cols = [col for col in feature_cols if col in df.columns]

X = df[final_feature_cols]
y = df[TARGET_COLUMN]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, stratify=y, random_state=42
)
logger.info(f"Data split: Train={len(X_train)}, Test={len(X_test)}")

scale_pos = y_train.value_counts()[0] / y_train.value_counts()[1]

xgb_base = XGBClassifier(
    objective="binary:logistic",
    eval_metric="logloss",
    scale_pos_weight=scale_pos,
    random_state=42,
    n_jobs=-1
)

param_dist = {
    "n_estimators": [500, 700, 1000],
    "max_depth": [6, 8, 10],
    "learning_rate": [0.01, 0.03, 0.05],
    "subsample": [0.5, 0.6, 0.7],              # Subsample baris rendah
    "colsample_bytree": [0.3, 0.4],            # Subsample fitur per pohon
    "colsample_bylevel": [0.5, 0.7, 0.9],      # Subsample fitur per level (BARU!)
    "gamma": [0, 1],
    "min_child_weight": [1, 3],
    "reg_alpha": [0, 0.1, 1],
    "reg_lambda": [1, 2, 3]
}

logger.info("⏳ Running RandomizedSearchCV for XGB...")
logger.info(f"Base scale_pos_weight: {scale_pos:.2f} (Should be near 1.0 after SMOTE)")

search = RandomizedSearchCV(
    estimator=xgb_base,
    param_distributions=param_dist,
    n_iter=50,
    cv=5,
    scoring="f1",
    verbose=1,
    n_jobs=1
)

search.fit(X_train, y_train)
best_model = search.best_estimator_
logger.info(f"🔥 Best Params: {search.best_params_}")

y_pred = best_model.predict(X_test)
y_proba = best_model.predict_proba(X_test)[:, 1]

acc = accuracy_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)
auc = roc_auc_score(y_test, y_proba)
rmse = mean_squared_error(y_test, y_proba, squared=False)
cm = confusion_matrix(y_test, y_pred)

report = f"""
==================== XGBoost OTD MODEL REPORT (TUNED) ====================
Total Train Rows: {len(X_train)}
Total Test Rows: {len(X_test)}
Target Distribution (Test Set): {Counter(y_test)}
Best Params: {search.best_params_}

Accuracy: {acc:.4f}
F1-Score: {f1:.4f}
ROC AUC: {auc:.4f}
RMSE: {rmse:.4f}

Confusion Matrix:
{cm}

Top Features:
{pd.Series(best_model.feature_importances_, index=X.columns).sort_values(ascending=False).to_string()}
"""

with open(OUTPUT_REPORT, "w") as f:
    f.write(report)

joblib.dump(best_model, OUTPUT_MODEL)
logger.info("💾 Model & report saved in /tmp/")
logger.info(f"🚀 Final → Acc:{acc:.4f} | F1:{f1:.4f} | AUC:{auc:.4f} | RMSE:{rmse:.4f}")
