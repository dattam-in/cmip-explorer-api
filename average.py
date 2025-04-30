"""
Metadata df columns: ['Dataset', 'Model', 'Scenario', 'Variant', 'Metric', 'File Name',
       'size_bytes', 'last_modified', 'Grid Info', 'Year']
"""

import json
import sys
import boto3
import pandas as pd
from pathlib import Path
from utils import get_spark_client

data_dir = Path("/data")
metadata_path = Path("./s3_metadata_v1.1_cleaned_v2.csv")
spark_tmp_dir = Path("/data/spark_tmp")
s3_bucket = "ceew-cmip6-data"
input_data = Path(data_dir) / "clipped-v3"
input_data.mkdir(parents=True, exist_ok=True)

s3 = boto3.client("s3")

with open("metrics.json", "r") as f:
    metrics = json.load(f)

metadata_df = pd.read_csv(metadata_path)


def run_average(metric, scenario):
    filtered_df = metadata_df[
        (metadata_df["Metric"] == metric) & (metadata_df["Scenario"] == scenario)
    ]
    years = filtered_df["Year"].unique()
    print(years)
    out_dir = Path(data_dir) / "ceew-ensemble" / metric / scenario
    out_dir.mkdir(parents=True, exist_ok=True)
    models = metrics[metric]["models"]
    year_file_paths = {}
    selected_df = filtered_df[filtered_df["Model"].isin(models)]
    # group by year and get files for each year
    for year in years:
        year_df = selected_df[selected_df["Year"] == year]
        # # assert that there is only one variant
        # assert len(year_df["Variant"].unique()) == 1
        # assert that there is only one grid info
        # assert len(year_df["Grid Info"].unique()) == 1
        file_paths = (
            (
                year_df["Dataset"]
                + "/"
                + year_df["Model"]
                + "/"
                + year_df["Scenario"]
                + "/"
                + year_df["Variant"]
                + "/"
                + year_df["Metric"]
                + "/"
                + year_df["File Name"]
            )
            .str.replace(".nc", ".parquet")
            .tolist()
        )
        file_paths = [input_data / file_path for file_path in file_paths]
        year_file_paths[year] = file_paths
    print(year_file_paths)
    for year, file_paths in year_file_paths.items():
        df = pd.concat([pd.read_parquet(file) for file in file_paths if file.exists()])
        df.drop(columns=["spatial_ref"])
        # mean of the metric across years, for each lat, lon
        mean_df = df.groupby(["lat", "lon", "time"]).mean().reset_index()
        mean_df["time"] = mean_df.time.dt.date
        mean_df["Model"] = "CEEW-ENSEMBLE"
        mean_df["Scenario"] = scenario
        mean_df.to_parquet(out_dir / f"{year}.parquet")


def sync_data(metric, scenario, model):
    file_rows = metadata_df[
        (metadata_df["Model"] == model)
        & (metadata_df["Scenario"] == scenario)
        & (metadata_df["Metric"] == metric)
    ]
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
    for scenario in ["ssp126", "ssp370", "ssp585"]:
        run_average(metric, scenario)
        break
