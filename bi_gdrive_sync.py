import os
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import io

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

def download_file_from_drive(file_id, file_name, local_folder, drive_service):
    """
    Download a file from Google Drive to a local folder.
    Args:
        file_id (str): The ID of the file to download.
        file_name (str): The name of the file to download.
        local_folder (str): The path of the local folder where the file will be saved.
        drive_service: Google Drive service object.
    """
    try:
        request = drive_service.files().get_media(fileId=file_id)
        local_file_path = os.path.join(local_folder, file_name)

        with io.FileIO(local_file_path, 'wb') as file:
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"\033[1;32mDownloading {file_name}: {int(status.progress() * 100)}%\033[0m")
        
        print(f"\033[1;32mDownloaded {file_name} to {local_folder}\033[0m")
    except HttpError as e:
        print(f"An error occurred while downloading file {file_name}: {e}")

def sync_local_to_drive_folders(folder_mapping, drive_service):

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

    print(f"\033[1;33mTotal Files Successfully Uploaded: {total_uploaded}\033[0m")
    return total_uploaded

def sync_drive_to_local_folders(folder_mapping, drive_service):

    total_downloaded = 0

    for idx, (local_folder, drive_folder_id) in enumerate(folder_mapping.items(), start=1):
        if not os.path.exists(local_folder):
            print(f"{idx}. Local folder not found, creating: {local_folder}")
            os.makedirs(local_folder)
        
        # List all files in the local folder
        local_files = {
            os.path.basename(file_path) for file_path in get_all_files_in_local_folder(local_folder)
        }
        
        # List all files in the corresponding Google Drive folder
        drive_files = list_files_in_drive_folder(drive_folder_id, drive_service)
        
        # Compare Drive files with local files and download missing ones
        for file_name, file_id in drive_files.items():
            if file_name not in local_files:
                print(f"{total_downloaded+1}. Syncing folder: Drive folder ID: {drive_folder_id} -> Local folder: {local_folder}")
                download_file_from_drive(file_id, file_name, local_folder, drive_service)
                total_downloaded += 1

    print(f"\033[1;33mTotal Files Successfully Downloaded: {total_downloaded}\033[0m")
    return total_downloaded