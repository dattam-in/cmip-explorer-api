from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import boto3
import xarray as xr
import pandas as pd
from typing import List, Optional
import json

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update this with your frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize AWS S3 client
s3_client = boto3.client('s3')
BUCKET_NAME = "nex-gddp-cmip6"

class DatasetQuery(BaseModel):
    model: str
    scenario: str
    ensemble: str
    variable: str
    year: int
    region: Optional[dict]  # GeoJSON format

@app.get("/api/metadata")
async def get_metadata():
    """
    Fetch and return the dataset metadata structure
    """
    try:
        # List objects in the bucket with the specified prefix
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix="NEX-GDDP-CMIP6/"
        )
        
        # Process the response to build metadata structure
        metadata = {
            "datasets": [],
            "models": set(),
            "scenarios": set(),
            "variables": set()
        }
        
        for obj in response.get('Contents', []):
            path_parts = obj.get('Key').split('/')
            if len(path_parts) >= 5:
                metadata["models"].add(path_parts[1])
                metadata["scenarios"].add(path_parts[2])
                metadata["variables"].add(path_parts[4])
        
        # Convert sets to sorted lists
        metadata["models"] = sorted(list(metadata["models"]))
        metadata["scenarios"] = sorted(list(metadata["scenarios"]))
        metadata["variables"] = sorted(list(metadata["variables"]))
        
        return metadata
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/download")
async def download_data(query: DatasetQuery):
    """
    Process and download climate data based on query parameters
    """
    try:
        # Construct S3 path
        file_path = f"NEX-GDDP-CMIP6/{query.model}/{query.scenario}/{query.ensemble}/{query.variable}/"
        file_name = f"{query.variable}_day_{query.model}_{query.scenario}_{query.ensemble}_gn_{query.year}.nc"
        
        # Download NetCDF file from S3
        response = s3_client.get_object(
            Bucket=BUCKET_NAME,
            Key=f"{file_path}{file_name}"
        )
        
        # Read NetCDF data
        ds = xr.open_dataset(response['Body'])
        
        # Apply regional filtering if specified
        if query.region:
            # Implement geometric filtering based on GeoJSON
            # This is a placeholder - actual implementation would depend on your needs
            ds = filter_by_region(ds, query.region)
        
        # Convert to pandas DataFrame
        df = ds.to_dataframe()
        
        # Save as parquet
        output_file = f"filtered_data_{query.year}.parquet"
        df.to_parquet(output_file)
        
        return {"message": "Data processed successfully", "file": output_file}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def filter_by_region(dataset, region):
    """
    Filter dataset by geographic region
    This is a placeholder - implement actual geometric filtering logic
    """
    # Implement regional filtering logic here
    return dataset

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
