import boto3
import csv
from pathlib import Path
from typing import List, Dict

def list_s3_objects(bucket_name: str, prefix: str = '') -> List[Dict]:
    """
    Recursively list all objects in an S3 bucket with the given prefix
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    
    all_objects = []
    
    # Handle pagination
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            all_objects.extend(page['Contents'])
    
    return all_objects

def extract_path_components(s3_key: str) -> List[str]:
    """
    Split S3 key into path components
    """
    return [component for component in s3_key.split('/') if component]

def find_max_depth(objects: List[Dict]) -> int:
    """
    Find the maximum directory depth among all objects
    """
    max_depth = 0
    for obj in objects:
        depth = len(extract_path_components(obj['Key']))
        max_depth = max(max_depth, depth)
    return max_depth

def create_metadata_csv(bucket_name: str, prefix: str, output_file: str):
    """
    Create a CSV file containing metadata about S3 objects with path components as columns
    """
    # Get all objects
    objects = list_s3_objects(bucket_name, prefix)
    
    if not objects:
        print(f"No objects found in s3://{bucket_name}/{prefix}")
        return
    
    # Find maximum depth to determine number of columns
    max_depth = find_max_depth(objects)
    
    # Prepare CSV headers
    headers = [f'path_component_{i}' for i in range(max_depth)]
    headers.extend(['size_bytes', 'last_modified'])
    
    # Create CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        
        for obj in objects:
            row_data = {}
            
            # Split the path into components
            path_components = extract_path_components(obj['Key'])
            
            # Fill path component columns
            for i in range(max_depth):
                if i < len(path_components):
                    row_data[f'path_component_{i}'] = path_components[i]
                else:
                    row_data[f'path_component_{i}'] = ''
            
            # Add metadata
            row_data['size_bytes'] = obj['Size']
            row_data['last_modified'] = obj['LastModified'].isoformat()
            
            writer.writerow(row_data)
    
    print(f"Metadata has been written to {output_file}")
    print(f"Processed {len(objects)} objects")

if __name__ == "__main__":
    # Configuration
    BUCKET_NAME = "nex-gddp-cmip6"
    PREFIX = "NEX-GDDP-CMIP6/"
    OUTPUT_FILE = "s3_metadata.csv"
    
    create_metadata_csv(BUCKET_NAME, PREFIX, OUTPUT_FILE)