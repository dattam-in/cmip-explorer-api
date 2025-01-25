import json
import sys
import boto3
import pandas as pd
from pathlib import Path
from utils import get_spark_client

data_dir = Path("/data")
metadata_path = Path("./s3_metadata_v1.1_cleaned.csv")
spark_tmp_dir = Path("/data/spark_tmp")
s3_bucket = ""
input_data = Path(data_dir) / "input"
input_data.mkdir(parents=True, exist_ok=True)

s3 = boto3.client("s3")

with open("metrics.json", "r") as f:
    metrics = json.load(f)

file_metadata = pd.read_csv(metadata_path)


def run_average(metric, scenario):
    models = metrics[metric]["models"]
    # sync data from s3 for all the models for  given metric and scenario
    for model in models:
        sync_data(metric, scenario, model)

    spark = get_spark_client(name="spark", tmp_dir=spark_tmp_dir)
    df = spark.read.parquet(str(input_data), recursiveFileLookup=True)
    print("loaded data")


def sync_data(metric, scenario, model):
    file_rows = file_metadata[
        (file_metadata["Model"] == model)
        & (file_metadata["Scenario"] == scenario)
        & (file_metadata["Metric"] == metric)
    ]
    print(file_rows)
    # copy files to tmp_data
    for _, row in file_rows.iterrows():
        s3_path = f"clipped/{row['Dataset']}/{row['Model']}/{row['Scenario']}/{row['Variant']}/{row['Metric']}/{row['File Name']}".replace(
            ".nc", ".parquet"
        )

        file_path = input_data / Path(
            f"{row['Dataset']}/{row['Model']}/{row['Scenario']}/{row['Metric']}/{row['File Name']}"
        ).with_suffix(".parquet")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if not (input_data / file_path).exists():
            s3.download_file(s3_bucket, s3_path, file_path)
            print(f"Downloaded {s3_path} to {file_path}")
        else:
            print(f"File {file_path} already exists in {input_data}")


if __name__ == "__main__":
    metric = sys.argv[1] if len(sys.argv) > 1 else "pr"
    scenario = sys.argv[2] if len(sys.argv) > 2 else "historical"
    run_average(metric, scenario)
