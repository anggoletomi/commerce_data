from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_function import *
from bi_gdrive_sync import *
from report_rc.rc_setup import *

def main_local_to_drive():
    drive_service = authenticate_google_sa(api_name='drive',api_version='v3',scopes=['https://www.googleapis.com/auth/drive'])

    updated_shopee_gdrive_folder = {os.path.join(os.getenv("BASE_RAW_FILE_PATH"), key): value for key, value in rc_shopee_gdrive_folder.items()}

    print(f"\033[1;33mTotal Folders Mapped in the Dictionary: {len(updated_shopee_gdrive_folder)}\033[0m")
    
    # Local status
    total_local_files = sum(len(get_all_files_in_local_folder(folder)) for folder in updated_shopee_gdrive_folder.keys())
    print(f"\033[1;33mLocal Status: {total_local_files} files\033[0m")

    # Drive status
    total_drive_files = sum(len(list_files_in_drive_folder(folder_id, drive_service)) for folder_id in updated_shopee_gdrive_folder.values())
    print(f"\033[1;33mDrive Status: {total_drive_files} files.\033[0m")

    # Sync the local folders with the corresponding Google Drive folders
    sync_local_to_drive_folders(updated_shopee_gdrive_folder, drive_service)

def main_drive_to_local():
    drive_service = authenticate_google_sa(api_name='drive',api_version='v3',scopes=['https://www.googleapis.com/auth/drive'])

    updated_shopee_gdrive_folder = {os.path.join(os.getenv("BASE_RAW_FILE_PATH"), key): value for key, value in rc_shopee_gdrive_folder.items()}

    print(f"\033[1;33mTotal Folders Mapped in the Dictionary: {len(updated_shopee_gdrive_folder)}\033[0m")
    
    # Local status
    total_local_files = sum(len(get_all_files_in_local_folder(folder)) for folder in updated_shopee_gdrive_folder.keys())
    print(f"\033[1;33mLocal Status: {total_local_files} files\033[0m")

    # Drive status
    total_drive_files = sum(len(list_files_in_drive_folder(folder_id, drive_service)) for folder_id in updated_shopee_gdrive_folder.values())
    print(f"\033[1;33mDrive Status: {total_drive_files} files\033[0m")

    # Sync Drive folders with the corresponding local folders
    sync_drive_to_local_folders(updated_shopee_gdrive_folder, drive_service)