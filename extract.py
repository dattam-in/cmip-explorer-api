from tqdm import tqdm
from pathlib import Path
import json
import pandas as pd
import xarray as xr
import geopandas as gpd
from shapely.geometry import mapping
import rioxarray
import boto3
import botocore

metrics_path = "metrics.json"

file_metadata_path = "s3_metadata.csv"

shapefile_path = "/home/ubuntu/process/India_District_Shapefile/India_Districts.shp"
"""
>>> df.columns
Index(['Dataset', 'Model', 'Scenario', 'Variant', 'Metric', 'File Name',
       'size_bytes', 'last_modified', 'is_v1_1'],
"""
cmip6_s3_bucket = "nex-gddp-cmip6"
dest_s3_bucket = ""
tmp_dir = Path("/data/input")
s3_client = boto3.client("s3")


def pick_only_latest_version(df):
    df["is_v1_1"] = df["File Name"].str.contains("v1.1")
    grouping_columns = ["Dataset", "Model", "Scenario", "Variant", "Metric"]
    has_v1_1 = df.groupby(grouping_columns)["is_v1_1"].any()
    keep_mask = df.apply(
        lambda row: (
            # Keep if this combination doesn't have v1.1
            not has_v1_1[tuple(row[col] for col in grouping_columns)]
            or
            # Or if this is the v1.1 version
            row["is_v1_1"]
        ),
        axis=1,
    )
    filtered_df = df[keep_mask].drop(columns=["is_v1_1"])
    # also drop rows where the file name doesn't end with .nc
    filtered_df = filtered_df[filtered_df["File Name"].str.endswith(".nc")]
    print(f"Filtered {len(filtered_df)} rows from {len(df)}")
    return filtered_df


def clip_india_data(file_path):
    tmp_path = tmp_dir / file_path
    out_path = Path(file_path.replace(".nc", ".parquet"))
    # if output file exists in s3, skip
    try:
        s3_client.head_object(Bucket=dest_s3_bucket, Key=f"clipped-v2/{out_path}")
        print(f"Skipping {file_path} as {out_path} already exists in s3")
        return
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            pass  # File doesn't exist, continue processing
        else:
            raise  # Re-raise if different error
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    s3_client.download_file(cmip6_s3_bucket, file_path, tmp_path)
    print(f"Downloaded {file_path} to {tmp_path}")
    try:
        clip_data(file_path, tmp_path, out_path)
    except Exception as e:
        print(f"Error clipping {file_path}: {e}")
        return
    # upload to sd3
    s3_client.upload_file(out_path, dest_s3_bucket, f"clipped-v2/{out_path}")
    out_path.unlink()
    print(f"Uploaded {out_path} to {dest_s3_bucket}")


def clip_data(file_path, tmp_path, out_path):
    data = xr.open_dataset(tmp_path, decode_times=False)
    data.rio.set_spatial_dims(x_dim="lon", y_dim="lat")

    data.rio.write_crs("epsg:4326", inplace=True)
    shapefile = gpd.read_file(
        shapefile_path,
        crs="epsg:4326",
    )
    clipped_data = data.rio.clip(
        shapefile.geometry.apply(mapping),
        shapefile.crs,
        drop=True,
        all_touched=True,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    clipped_df = clipped_data.to_dataframe()
    vars = list(clipped_data.data_vars)
    assert len(vars) == 1
    metric = vars[0]
    # drop rows where the metric is nan (grids that are outside the shapefile but still in the bounding box of india are included with NaN values)
    clipped_df = clipped_df[~clipped_df[metric].isna()]
    clipped_df.to_parquet(out_path)
    # create geojson

    tmp_path.unlink()
    print(f"Clipped {file_path} to {out_path}")


def run():
    metrics = json.load(open(metrics_path))
    file_metadata = pd.read_csv(file_metadata_path)
    file_metadata = pick_only_latest_version(file_metadata)
    for metric, md in tqdm(metrics.items(), desc="Processing metrics"):
        rows = file_metadata[
            (file_metadata["Metric"] == metric)
            & file_metadata["Model"].isin(md["models"])
        ]
        print(f"Processing {metric} with {len(rows)} files")
        for _, row in tqdm(
            rows.iterrows(), total=len(rows), desc=f"Processing {metric}"
        ):
            file_path = f"{row['Dataset']}/{row['Model']}/{row['Scenario']}/{row['Variant']}/{row['Metric']}/{row['File Name']}"
            clip_india_data(file_path)
        break


if __name__ == "__main__":
    run()
