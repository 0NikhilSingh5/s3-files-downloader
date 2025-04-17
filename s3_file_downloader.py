import boto3  # AWS SDK for Python
from datetime import datetime, timedelta  # For date manipulation
import os  # For operating system functionality
from pathlib import Path  # For easier path handling

def list_objects(bucket_name, folder_prefix, days_back,name_filter=None):
    """
    List objects in an S3 bucket that were modified within a specified number of days.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        folder_prefix (str): Prefix/folder path to filter objects
        days_back (int): Number of days to look back for modified files
        
    Returns:
        list: Sorted list of objects, newest first
    """
    s3 = boto3.client("s3")
    cutoff_date = datetime.now() - timedelta(days=days_back)
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
    all_objects = []
    for page in pages:
        all_objects.extend(page.get('Contents', []))  
    
    # Filter objects by modified date
    filtered_objects = [
        obj for obj in all_objects if obj.get("LastModified").replace(tzinfo=None) >= cutoff_date
    ]
    
    if name_filter:
        filtered_objects = [
            obj for obj in filtered_objects if name_filter.lower() in obj["Key"].lower()
        ]
    # Sort objects by LastModified date(newest first)
    return sorted(filtered_objects, key=lambda x: x["LastModified"], reverse=True)

def download_files(bucket_name, files_list):
    """
    Download files from S3 bucket to local directory with progress reporting.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        files_list (list): List of file objects to download
    """
    s3 = boto3.client("s3") 
    download_dir = Path(os.getcwd()) / "downloads"
    download_dir.mkdir(exist_ok=True)  # Create directory if it doesn't exist
    
    total_files = len(files_list)
    
    for obj in files_list:
        file_key = obj['Key']
        file_name = os.path.basename(file_key) or file_key.replace('/', '_')
        local_path = download_dir / file_name
        print(f"Downloading {file_key} -> {local_path} ({index}/{total_files})")
        try:
            s3.download_file(bucket_name, file_key, str(local_path))
            print(f"✓ Success: {file_name}")
        except Exception as e:
            print(f"✗ Error: {str(e)}")

def main():
    """
    Main function to execute the script.
    """
    bucket_name = "readywire-private"  # S3 bucket name
    folder_prefix = "JS4W839K/M&M/Prod/RPAImports/"  # Folder path within bucket
    days_back = 3  # Look back period in days
    
    print(f"Listing files from last {days_back} days...")
    files = list_objects(bucket_name, folder_prefix, days_back)  # Get files list
    
    if files:
        print(f"Found {len(files)} files. Downloading...")
        download_files(bucket_name, files)  # Download all files
    else:
        print("No files found.")

if __name__ == "__main__":  # Fixed syntax error here
    main()  # Run the main function when script is executed directly