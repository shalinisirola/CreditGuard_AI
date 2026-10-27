from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.ml.functions import vector_to_array
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.ml.feature import VectorAssembler
import mlflow
import mlflow.spark

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
FRAUD_REVIEW_THRESHOLD = 0.7
import socket

def is_hdfs_available(host="namenode", port=9000, timeout=2):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

if is_hdfs_available():
    HDFS_BASE_PATH = "hdfs://namenode:9000/data/fraud"
    CHECKPOINT_BASE_PATH = "hdfs://namenode:9000/checkpoints/fraud"
else:
    print("⚠️ HDFS (namenode:9000) not reachable. Falling back to local storage checkpoints.")
    HDFS_BASE_PATH = "/tmp/data/fraud"
    CHECKPOINT_BASE_PATH = "/tmp/checkpoints/fraud"

FEATURE_COLUMNS = [f"V{i}" for i in range(1, 29)] + ["Amount"]

spark = SparkSession.builder.appName("FraudDetectorRealTime").getOrCreate()

input_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("event_timestamp", DoubleType(), False),
    StructField("source", StringType(), True),
    *[StructField(column, DoubleType(), False) for column in FEATURE_COLUMNS],
])

incoming = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

parsed = incoming.select(
    from_json(col("value").cast("string"), input_schema).alias("transaction")
).select("transaction.*")

assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
features = assembler.transform(parsed)

mlflow.set_tracking_uri("http://mlflow:5000")
client = mlflow.tracking.MlflowClient()
experiment = client.get_experiment_by_name("kaggle_fraud_detection")
if experiment is None:
    raise RuntimeError("kaggle_fraud_detection does not exist. Train the Kaggle model first.")
runs = client.search_runs(experiment.experiment_id, order_by=["start_time DESC"], max_results=1)
if not runs:
    raise RuntimeError("No trained Kaggle model exists. Run train_model_kaggle.py first.")

model_uri = f"runs:/{runs[0].info.run_id}/kaggle_fraud_model"
model = mlflow.spark.load_model(model_uri)
predictions = model.transform(features)
scored = predictions.select(
    "transaction_id", "user_id", "source", "event_timestamp", "Amount",
    "prediction", "probability"
).withColumn("fraud_score", vector_to_array(col("probability"))[1]) \
 .withColumn("review_required", col("fraud_score") >= FRAUD_REVIEW_THRESHOLD) \
 .withColumn("scored_timestamp", current_timestamp()) \
 .withColumn(
     "latency_ms",
     (col("scored_timestamp").cast("double") - col("event_timestamp")) * 1000
 )


def kafka_sink(dataframe, topic, checkpoint):
    return dataframe.selectExpr("to_json(struct(*)) AS value").writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint) \
        .trigger(processingTime="1 second") \
        .start()


all_predictions = kafka_sink(scored, "all_predictions", f"{CHECKPOINT_BASE_PATH}/all")
fraud_alerts = kafka_sink(
    scored.filter(col("review_required")),
    "fraud_alerts",
    f"{CHECKPOINT_BASE_PATH}/alerts",
)

raw_storage = parsed.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", f"{HDFS_BASE_PATH}/raw") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/raw") \
    .trigger(processingTime="1 second") \
    .start()

scored_storage = scored.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", f"{HDFS_BASE_PATH}/scored") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/scored") \
    .trigger(processingTime="1 second") \
    .start()

print("Real-time Kaggle-compatible fraud scoring is running")
spark.streams.awaitAnyTermination()
