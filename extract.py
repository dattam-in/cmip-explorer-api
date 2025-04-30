from calendar import c
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
from concurrent.futures import ProcessPoolExecutor

metrics_path = "metrics.json"

file_metadata_path = "s3_metadata_v1.1_cleaned_v2.csv"
out_dir = Path("/data/clipped-v3")

shapefile_path = "/home/ubuntu/process/India_District_Shapefile/India_Districts.shp"
"""
>>> df.columns
Index(['Dataset', 'Model', 'Scenario', 'Variant', 'Metric', 'File Name',
       'size_bytes', 'last_modified', 'is_v1_1'],
"""
cmip6_s3_bucket = "nex-gddp-cmip6"
dest_s3_bucket = "ceew-cmip6-data"
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
    out_path = out_dir / Path(file_path.replace(".nc", ".parquet"))
    if out_path.exists():
        print(f"Skipping {file_path} as {out_path} already exists")
        return
    # if output file exists in s3, skip
    # try:
    #     s3_client.head_object(Bucket=dest_s3_bucket, Key=f"clipped-v2/{out_path}")
    #     print(f"Skipping {file_path} as {out_path} already exists in s3")
    #     return
    # except botocore.exceptions.ClientError as e:
    #     if e.response["Error"]["Code"] == "404":
    #         pass  # File doesn't exist, continue processing
    #     else:
    #         raise  # Re-raise if different error
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    if not tmp_path.exists():
        s3_client.download_file(cmip6_s3_bucket, file_path, tmp_path)
        print(f"Downloaded {file_path} to {tmp_path}")
    try:
        clip_data(tmp_path, out_path)
    except Exception as e:
        print(f"Error clipping {file_path}: {e}")
        return
    tmp_path.unlink()
    # upload to s3
    # s3_client.upload_file(out_path, dest_s3_bucket, f"clipped-v2/{out_path}")
    # out_path.unlink()
    # print(f"Uploaded {out_path} to {dest_s3_bucket}")


def clip_data_by_shapefile(data, shapefile_path):
    shapefile = gpd.read_file(shapefile_path)
    data.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    data.rio.write_crs("epsg:4326", inplace=True)
    clipped_data = data.rio.clip(
        shapefile.geometry.apply(mapping), shapefile.crs, drop=True, all_touched=True
    )
    return clipped_data


def clip_data(tmp_path, out_path):
    data = xr.open_dataset(
        tmp_path, decode_times=xr.coders.CFDatetimeCoder(use_cftime=True)
    )
    clipped_data = clip_data_by_shapefile(data, shapefile_path)
    calendar_type = clipped_data.indexes["time"].calendar

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if (
        not isinstance(clipped_data.indexes["time"], pd.DatetimeIndex)
        and calendar_type == "365_day"
    ):
        clipped_data["time"] = clipped_data.indexes["time"].to_datetimeindex(
            unsafe=True
        )
    clipped_df = clipped_data.to_dataframe().reset_index()
    vars = list(clipped_data.data_vars)
    assert (
        len(vars) == 1
    ), "since we are dropping rows with NaN values, making sure only one variable is present"
    metric = vars[0]
    # drop rows where the metric is nan (grids that are outside the shapefile but still in the bounding box of india are included with NaN values)
    clipped_df = clipped_df[~clipped_df[metric].isna()]
    assert clipped_df["spatial_ref"].unique().tolist() == [0], "Spatial ref is not 0"
    if calendar_type == "360_day":
        # datetimes for 360_day are not convertible as it has values like 2014-02-30
        # in these cases, tiem column is stored as string in the final parquet file
        tmp_csv_path = out_path.with_suffix(".csv")
        clipped_df.to_csv(tmp_csv_path, index=False)
        clipped_df = pd.read_csv(tmp_csv_path)

        tmp_csv_path.unlink()
    clipped_df.to_parquet(out_path)

    print(f"Clipped {tmp_path} to {out_path}")


def run():
    metrics = json.load(open(metrics_path))
    file_metadata = pd.read_csv(file_metadata_path)

    for metric, md in tqdm(metrics.items(), desc="Processing metrics"):
        rows = file_metadata[
            (file_metadata["Metric"] == metric)
            & file_metadata["Model"].isin(md["models"])
            & (
                file_metadata["File Name"]
                == "pr_day_HadGEM3-GC31-MM_ssp126_r1i1p1f3_gn_2018_v1.1.nc"
            )
        ]
        print(f"Processing {metric} with {len(rows)} files")
        with ProcessPoolExecutor(max_workers=8) as executor:
            file_paths = [
                f"{row['Dataset']}/{row['Model']}/{row['Scenario']}/{row['Variant']}/{row['Metric']}/{row['File Name']}"
                for _, row in rows.iterrows()
            ]
            list(
                tqdm(
                    executor.map(clip_india_data, file_paths),
                    total=len(file_paths),
                    desc=f"Processing {metric}",
                )
            )
        break


if __name__ == "__main__":
    run()
