import boto3
from datetime import datetime, timedelta
import os
from pathlib import Path

def list_objects(bucket_name, folder_prefix, days_back):
    s3 = boto3.client("s3")
    cutoff_date = datetime.now() - timedelta(days=days_back)
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)

    all_objects = []
    for page in pages:
        all_objects.extend(page.get('Contents', []))

    filtered_objects = [
        obj for obj in all_objects if obj.get("LastModified").replace(tzinfo=None) >= cutoff_date
    ]
    
    return sorted(filtered_objects, key=lambda x: x["LastModified"], reverse=True)

def download_files(bucket_name, files_list):
    s3 = boto3.client("s3")
    download_dir = Path(os.getcwd()) / "downloads"
    download_dir.mkdir(exist_ok=True)

    for obj in files_list:
        file_key = obj['Key']
        file_name = os.path.basename(file_key) or file_key.replace('/', '_')
        local_path = download_dir / file_name
        print(f"Downloading {file_key} -> {local_path}")
        s3.download_file(bucket_name, file_key, str(local_path))

def main():
    bucket_name = "readywire-private"
    folder_prefix = "JS4W839K/M&M/Prod/RPAImports/"
    days_back = 3

    print(f"Listing files from last {days_back} days...")
    files = list_objects(bucket_name, folder_prefix, days_back)

    if files:
        print(f"Found {len(files)} files. Downloading...")
        download_files(bucket_name, files)
    else:
        print("No files found.")

if __name__ == "__main__":
    main()
