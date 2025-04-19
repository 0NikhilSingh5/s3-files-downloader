import boto3
from datetime import datetime, timedelta
import os
from pathlib import Path
import re  # Added for date validation

def list_objects(bucket_name, folder_prefix, days_back=None, specific_date=None, name_filter=None):
    """
    List objects in an S3 bucket based on time criteria.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        folder_prefix (str): Prefix/folder path to filter objects
        days_back (int, optional): Number of days to look back for modified files
        specific_date (datetime, optional): Specific date to filter files
        name_filter (str, optional): String pattern to filter filenames
        
    Returns:
        list: Sorted list of objects, newest first
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
    all_objects = []
    for page in pages:
        all_objects.extend(page.get('Contents', []))
    
    # Filter by time criteria
    filtered_objects = []
    
    if days_back is not None:
        # Filter by days back
        cutoff_date = datetime.now() - timedelta(days=days_back)
        filtered_objects = [
            obj for obj in all_objects if obj.get("LastModified").replace(tzinfo=None) >= cutoff_date
        ]
    elif specific_date is not None:
        # Filter by specific date
        start_of_day = datetime(specific_date.year, specific_date.month, specific_date.day, 0, 0, 0)
        end_of_day = datetime(specific_date.year, specific_date.month, specific_date.day, 23, 59, 59)
        
        filtered_objects = [
            obj for obj in all_objects 
            if start_of_day <= obj.get("LastModified").replace(tzinfo=None) <= end_of_day
        ]
    else:
        # No time filter
        filtered_objects = all_objects
    
    # Apply name filter if provided
    if name_filter:
        filtered_objects = [
            obj for obj in filtered_objects if name_filter.lower() in obj["Key"].lower()
        ]
    
    # Sort objects by LastModified date
    return sorted(filtered_objects, key=lambda x: x["LastModified"], reverse=True)

def get_specific_date():
    """Prompt user for a specific date in DD-MM-YYYY format"""
    date_pattern = re.compile(r'^(\d{2})-(\d{2})-(\d{4})$')
    
    while True:
        date_str = input("\nEnter date (DD-MM-YYYY): ").strip()
        match = date_pattern.match(date_str)
        
        if not match:
            print("Invalid date format. Please use DD-MM-YYYY format.")
            continue
            
        try:
            day, month, year = map(int, [match.group(1), match.group(2), match.group(3)])
            date_obj = datetime(year, month, day)
            return date_obj
        except ValueError:
            print("Invalid date. Please enter a valid date.")

def download_files(bucket_name, files_list, custom_directory=None):
    """
    Download files from S3 bucket to local directory with progress reporting.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        files_list (list): List of file objects to download
        custom_directory (str, optional): Custom directory name for downloads
    """
    s3 = boto3.client("s3")
    
    # Allow custom download directory
    dir_name = custom_directory or "downloads"
    download_dir = Path(os.getcwd()) / dir_name
    download_dir.mkdir(exist_ok=True)
    
    total_files = len(files_list)
    successful = 0
    failed = 0
    
    for index, obj in enumerate(files_list, 1):
        file_key = obj['Key']
        file_name = os.path.basename(file_key) or file_key.replace('/', '_')
        local_path = download_dir / file_name
        
        print(f"Downloading {file_key} -> {local_path} ({index}/{total_files})")
        
        try:
            s3.download_file(bucket_name, file_key, str(local_path))
            print(f"✓ Success: {file_name}")
            successful += 1
        except Exception as e:
            print(f"✗ Error: {str(e)}")
            failed += 1
    
    print(f"\n✅ Download completed. Success: {successful}, Failed: {failed}")

def display_file_info(files_list):
    """
    Display information about found files including size.
    
    Args:
        files_list (list): List of file objects
    """
    if not files_list:
        print("No files found.")
        return
    
    print(f"\nFound {len(files_list)} files:")
    print("-" * 80)
    
    total_size = 0
    # Add index numbers for selection
    for idx, obj in enumerate(files_list, 1):
        file_size_kb = obj.get('Size', 0) / 1024
        total_size += file_size_kb
        print(f"{idx}. {obj['Key']} ({file_size_kb:.2f} KB, Last Modified: {obj['LastModified']})")
    
    print("-" * 80)
    print(f"Total size: {total_size:.2f} KB ({total_size/1024:.2f} MB)")
    
    return files_list

def main():
    """
    Main function to execute the script.
    """
    bucket_name = "readywire-private"
    folder_prefix = "JS4W839K/M&M/Prod/RPAImports/"
    
    print("S3 File Manager")
    print("--------------")
    
    # Get listing mode from user
    print("\nHow would you like to list files?")
    print("1. Files from the last X days")
    print("2. Files from a specific date")
    
    mode = input("\nEnter your choice (1 or 2): ").strip()
    files = []
    
    if mode == '1':
        days_back = int(input("\nEnter number of days to look back: ").strip())
        name_filter = input("Enter name filter (optional): ").strip() or None
        
        print(f"\nListing files from last {days_back} days...")
        files = list_objects(bucket_name, folder_prefix, days_back=days_back, name_filter=name_filter)
    
    elif mode == '2':
        specific_date = get_specific_date()
        name_filter = input("Enter name filter (optional): ").strip() or None
        
        print(f"\nListing files from {specific_date.strftime('%Y-%m-%d')}...")
        files = list_objects(bucket_name, folder_prefix, specific_date=specific_date, name_filter=name_filter)
    
    else:
        print("Invalid choice. Defaulting to last 3 days.")
        files = list_objects(bucket_name, folder_prefix, days_back=3)
    
    # Display files
    display_file_info(files)
    
    if files:
        download_choice = input("\nDownload files? (y/n): ").lower().strip()
        if download_choice == 'y':
            custom_dir = input("\nEnter download directory name (leave blank for 'downloads'): ").strip()
            download_files(bucket_name, files, custom_dir or None)
    
    print("\nThank you for using S3 File Manager. Goodbye!")

if __name__ == "__main__":
    main()