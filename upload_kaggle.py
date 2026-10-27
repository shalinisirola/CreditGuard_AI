"""
Upload Kaggle Credit Card Fraud Dataset to HDFS
================================================
This script copies creditcard.csv into the Docker HDFS data lake.
"""

import subprocess
from pathlib import Path

dataset = Path("creditcard.csv")
if not dataset.exists():
    raise SystemExit("Error: creditcard.csv was not found in the current directory.")

print("Uploading Kaggle dataset to HDFS...")

try:
    subprocess.run(["docker", "cp", str(dataset), "namenode:/tmp/creditcard.csv"], check=True)
    subprocess.run(["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", "/data"], check=True)
    subprocess.run([
        "docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f",
        "/tmp/creditcard.csv", "/data/creditcard.csv"
    ], check=True)
    print("Successfully uploaded creditcard.csv to hdfs://namenode:9000/data/creditcard.csv")
except FileNotFoundError:
    print("Error: Docker is required and must be available on PATH.")
except subprocess.CalledProcessError as error:
    print(f"Error uploading dataset to HDFS: {error}")
