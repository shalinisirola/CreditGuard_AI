# 🛡️ CreditGuard AI - Real-Time Fraud Detection System

A production-grade, end-to-end machine learning system for detecting credit card fraud in real-time using Apache Spark, Kafka, and MLflow.


## 📊 Live Dashboard

Real-time monitoring dashboard built with Streamlit showing:
- Transaction volume and fraud detection metrics
- Interactive charts (Pie, Line, Bar)

- Geographic fraud distribution

## 📸 Live System Preview

### Real-Time Dashboard
![Dashboard - Top](screenshots/dashboard_top.png)
![Dashboard - Charts](screenshots/dashboard_bottom.png)

### MLflow Experiment Tracking
![MLflow - Kaggle Model](screenshots/mlflow_kaggle.png)
![MLflow - Synthetic Model](screenshots/mlflow_synthetic.png)


## 🏗️ Architecture

```
Producer → Kafka → Spark Streaming → ML Model → Predictions → Dashboard
                       ↓
                     HDFS data lake
                      ↓
                   MLflow (Model Registry)
```

### Components

- **Data Ingestion**: Kafka for high-throughput streaming
- **Stream Processing**: Apache Spark Structured Streaming
- **ML Framework**: PySpark MLlib (Random Forest Classifier)
- **Model Management**: MLflow for experiment tracking and versioning
- **Storage**: HDFS for transaction and training data; MLflow artifacts use a shared Docker volume
- **Scoring API**: FastAPI endpoint at `POST /score`
- **Visualization**: Streamlit dashboard

## 🚀 Features

-  **Real-time fraud detection** with a 0.7 manual-review threshold and a one-second processing trigger
-  **Multi-source schema mapping** (supports multiple bank formats)
-  **Dual model training** (synthetic + Kaggle dataset)
-  **Model versioning** with MLflow
-  **Interactive dashboard** with live metrics
-  **Scalable architecture** (Kafka + Spark)
-  **Dockerized** for easy deployment

## 📈 Model Performance

| Model | AUC Score | Fraud Detection Rate | Dataset |
|-------|-----------|----------------------|---------|
| Synthetic | ~100% | ~100% | Synthetic (streaming demo) |
| Kaggle | 95.6% | 68.9% | 284K real transactions |

> **Note on Validation:** The synthetic model demonstrates the real-time streaming architecture but is not a rigorous ML validation (trained and tested on same distribution). The **Kaggle model** provides realistic performance metrics on actual credit card fraud data. This project's primary contribution is the **production-grade streaming infrastructure** and **MLOps implementation**, not the ML model itself.

## 🛠️ Tech Stack

**Core:**
- Python 3.9
- Apache Spark 3.4.1
- Apache Kafka 7.4.0
- MLflow 2.7.1
- Streamlit

**Infrastructure:**
- Docker & Docker Compose
- Docker named volume for MLflow artifacts


**ML Libraries:**
- PySpark MLlib
- Pandas
- Scikit-learn

## 📦 Installation

### Prerequisites
- Docker Desktop
- Python 3.9+


### Quick Start

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/CreditGuard_AI.git
cd CreditGuard_AI
```

2. **Start infrastructure services, including the HDFS NameNode and DataNode**
```bash
docker --version
docker compose version
```

3. **Install local producer dependencies**
```bash
pip install -r requirements.txt
```

4. **Upload the Kaggle file to HDFS before training**

```bash
docker cp creditcard.csv namenode:/tmp/creditcard.csv
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put -f /tmp/creditcard.csv /data/creditcard.csv
```


## 🎯 Training Models

### Synthetic Model
Run the training script locally with Spark:
```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 train_model.py
```

### Kaggle Model
```bash
# Upload dataset to HDFS as shown above
python upload_kaggle.py

# Train
docker cp train_model_kaggle.py spark-master:/opt/spark/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/train_model_kaggle.py
```

The `spark-inference` and `scoring-api` services start from Docker Compose. Restart inference after training:

```bash
docker compose restart spark-inference
python producer.py
```

The API accepts the same `V1`-`V28` and `Amount` features as the model:

```bash
curl -X POST http://localhost:8000/score -H "Content-Type: application/json" -d @transaction.json
```

## 📁 Project Structure

```
CreditGuard_AI/
├── producer.py                 # Kaggle-compatible Kafka transaction replay
├── spark_inference.py          # Real-time inference
├── train_model_kaggle.py       # Kaggle model training
├── api.py                      # Synchronous realtime scoring API
├── dashboard.py                # Streamlit UI
├── schema_config.py            # Multi-source schema mapping
├── docker-compose.yml          # Infrastructure setup
├── requirements.txt            # Python dependencies
├── SCHEMA_MAPPING_GUIDE.md     # Adding new data sources
└── README.md                   # This file
```

## 🔧 Configuration

### Schema Mapping
To add support for a new bank's data format, edit `schema_config.py`:
```python
SCHEMA_MAPPINGS = {
    "new_bank": {
        "user_id": "customer_id",
        "amount": "transaction_value",
        ...
    }
}
```

See `SCHEMA_MAPPING_GUIDE.md` for detailed instructions.

## 📊 How It Works

1. **Data Generation**: `producer.py` replays Kaggle feature rows and sends them to Kafka
2. **Stream Processing**: Spark reads the Kafka stream using the trained model's exact feature contract
3. **Prediction**: The model emits a fraud probability; transactions at or above 0.7 are flagged for manual review
4. **API response**: `POST /score` waits for the matching Kafka prediction and returns within a two-second deadline
5. **Output**: Results sent to Kafka topics and displayed on dashboard

## 🎓 Learning Outcomes

This project demonstrates:
-  Real-time stream processing at scale
-  ML model deployment in production
-  Microservices architecture
-  MLOps best practices
-  Data pipeline orchestration
-  Full-stack development (backend + dashboard)


## 📝 License

MIT License - feel free to use for learning and portfolio purposes.

**Note:** The original `creditcard.csv` dataset is not included in this repository due to GitHub's file-size limit. Download it from the Kaggle Credit Card Fraud Detection dataset and upload it to HDFS with `upload_kaggle.py`.
---

** If this project helped you, please give it a star!**
