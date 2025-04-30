# load metadata
# go through each parquet file and add metadata columns
# Model, Scenario
# save to new parquet file in clipped-v4


from pathlib import Path
import pandas as pd

data_dir = Path("/data")
metadata_path = Path("./s3_metadata_v1.1_cleaned_v2.csv")
spark_tmp_dir = Path("/data/spark_tmp")
s3_bucket = "ceew-cmip6-data"
input_data = Path(data_dir) / "clipped-v3"
input_data.mkdir(parents=True, exist_ok=True)
output_data = Path(data_dir) / "clipped-v3-enriched"
metadata_df = pd.read_csv(metadata_path)

from concurrent.futures import ProcessPoolExecutor
from functools import partial


def process_file(row, input_data, output_data):
    model = row["Model"]
    scenario = row["Scenario"]
    nc_file_name = row["File Name"]
    pq_file_name = Path(nc_file_name).with_suffix(".parquet")
    file_path = f"{row['Dataset']}/{row['Model']}/{row['Scenario']}/{row['Variant']}/{row['Metric']}/{pq_file_name}"
    pq_file_path = input_data / file_path
    output_path = output_data / file_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if pq_file_path.exists():
        print(pq_file_path)
        df = pd.read_parquet(pq_file_path).reset_index()
        df["time"] = pd.to_datetime(df["time"]).dt.date
        df["Model"] = model
        df["Scenario"] = scenario
        df.to_parquet(output_path)


with ProcessPoolExecutor(max_workers=10) as executor:
    process_func = partial(process_file, input_data=input_data, output_data=output_data)
    executor.map(process_func, [row for _, row in metadata_df.iterrows()])
