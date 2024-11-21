from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_function import *
from rc_report.rc_setup import *

from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

def list_files_in_drive_folder(folder_id, drive_service):
    """
    List all files in a specific Google Drive folder, handling pagination.
    Args:
        folder_id (str): ID of the Google Drive folder.
        drive_service: Google Drive service object.
    Returns:
        dict: A dictionary with file names as keys and their file IDs as values.
    """
    try:
        files = {}
        query = f"'{folder_id}' in parents and trashed = false"
        page_token = None

        while True:
            response = drive_service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name)',
                pageToken=page_token
            ).execute()
            for file in response.get('files', []):
                files[file['name']] = file['id']

            page_token = response.get('nextPageToken')
            if not page_token:
                break
        return files
    except HttpError as e:
        print(f"An error occurred while listing files in Drive folder {folder_id}: {e}")
        return {}

def get_all_files_in_local_folder(local_folder):
    """
    Recursively get all files in a local folder.
    Args:
        local_folder (str): Path to the local folder.
    Returns:
        set: A set of full file paths for all files in the folder.
    """
    file_paths = set()
    for root, _, files in os.walk(local_folder):
        for file_name in files:
            file_paths.add(os.path.join(root, file_name))
    return file_paths

def upload_file_to_drive(file_path, folder_id, drive_service):
    """
    Upload a file to a specific Google Drive folder.
    Args:
        file_path (str): Path of the file to upload.
        folder_id (str): ID of the folder where the file will be uploaded.
        drive_service: Google Drive service object.
    Returns:
        File ID of the uploaded file.
    """
    file_name = os.path.basename(file_path)
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, resumable=True)

    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()
    
    print(f"\033[1;32mUploaded {file_name} to folder {folder_id} with File ID: {file['id']}\033[0m")
    return file['id']

def sync_local_and_drive_folders(folder_mapping, drive_service):

    total_uploaded = 0

    for idx, (local_folder, drive_folder_id) in enumerate(folder_mapping.items(), start=1):
        if not os.path.exists(local_folder):
            print(f"{idx}. Local folder not found: {local_folder}")
            continue
        
        # List all files in the local folder (no subfolders)
        local_files = [
            file_name
            for file_name in os.listdir(local_folder)
            if os.path.isfile(os.path.join(local_folder, file_name))
        ]
        
        # List all files in the corresponding Google Drive folder
        drive_files = list_files_in_drive_folder(drive_folder_id, drive_service)
        drive_file_names = set(drive_files.keys())
    
        # Compare local files with Drive files and upload missing ones
        for file_name in local_files:
            if file_name not in drive_file_names:
                file_path = os.path.join(local_folder, file_name)
                print(f"{total_uploaded+1}. Syncing folder: {local_folder} -> Drive folder ID: {drive_folder_id}")
                upload_file_to_drive(file_path, drive_folder_id, drive_service)
                total_uploaded += 1

    print(f"\033[1;33mTotal Files Success Upload: {total_uploaded}\033[0m")
    return total_uploaded

if __name__ == '__main__':
    drive_service = authenticate_google_sa(
        api_name='drive',
        api_version='v3',
        scopes=['https://www.googleapis.com/auth/drive']
    )

    updated_shopee_gdrive_folder = {os.path.join(os.getenv("BASE_RAW_FILE_PATH"), key): value for key, value in shopee_gdrive_folder.items()}

    print(f"\033[1;33mTotal Folders Mapped in the Dictionary: {len(updated_shopee_gdrive_folder)}\033[0m")
    
    # Local status
    total_local_files = sum(len(get_all_files_in_local_folder(folder)) for folder in updated_shopee_gdrive_folder.keys())
    print(f"\033[1;33mLocal Status: {total_local_files} files\033[0m")

    # Drive status
    total_drive_files = sum(len(list_files_in_drive_folder(folder_id, drive_service)) for folder_id in updated_shopee_gdrive_folder.values())
    print(f"\033[1;33mDrive Status: {total_drive_files} files.\033[0m")

    # Sync the local folders with the corresponding Google Drive folders
    sync_local_and_drive_folders(updated_shopee_gdrive_folder, drive_service)
