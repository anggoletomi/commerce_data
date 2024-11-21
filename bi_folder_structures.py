from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

import json

def export_folder_structure(base_path, output_file):
    folder_dict = {}
    main_folder = os.path.basename(base_path)

    # Walk through the directory and build a nested dictionary of folders
    for root, dirs, files in os.walk(base_path):
        relative_path = os.path.relpath(root, base_path)
        folder_dict[relative_path] = dirs

    # Wrap folder structure under the main folder name
    export_dict = {main_folder: folder_dict}

    # Save the folder structure as JSON
    with open(output_file, 'w') as f:
        json.dump(export_dict, f, indent=4)
    print(f"Folder structure exported to {output_file}")

def update_folder_structure(target_path, structure_file):
    # Load the folder structure from JSON
    with open(structure_file, 'r') as f:
        export_dict = json.load(f)

    # Extract the main folder and its structure
    main_folder, folder_dict = next(iter(export_dict.items()))
    main_folder_path = os.path.join(target_path, main_folder)
    os.makedirs(main_folder_path, exist_ok=True)

    # Keep track of all necessary folders from JSON structure
    required_paths = set()

    # Helper function to create folders as per the JSON structure and record required paths
    def create_folders_from_json():
        for relative_path, subfolders in folder_dict.items():
            full_path = os.path.join(main_folder_path, relative_path)
            os.makedirs(full_path, exist_ok=True)
            required_paths.add(full_path)
            for subfolder in subfolders:
                subfolder_path = os.path.join(full_path, subfolder)
                os.makedirs(subfolder_path, exist_ok=True)
                required_paths.add(subfolder_path)

    # Helper function to delete only the latest (deepest) empty sub-folder if it's not in the new structure
    def delete_latest_extra_folder(current_path):
        if not os.path.exists(current_path):
            return  # Skip non-existent paths

        # Traverse to subdirectories first to reach the latest (deepest) sub-folder
        for folder in os.listdir(current_path):
            folder_path = os.path.join(current_path, folder)

            if os.path.isdir(folder_path):
                # Recursively go deeper into subdirectories
                delete_latest_extra_folder(folder_path)

                # After checking subdirectories, delete only if the folder is not required and is empty
                if folder_path not in required_paths:
                    contents = os.listdir(folder_path)
                    has_files = any(os.path.isfile(os.path.join(folder_path, item)) for item in contents)

                    # Only delete if it has no files and no subfolders
                    if has_files:
                        print(f"\033[1;31m\033[1mThis path '{folder_path}' has files, but it is no longer in the new folder structure. Kindly check and do manual necessary adjustments.\033[0m")
                    elif not contents:  # Delete if truly empty (no files, no subfolders)
                        os.rmdir(folder_path)
                        print(f"Removed empty latest sub-folder that is no longer in the new folder structure: {folder_path}")

    # Step 1: Create folders based on the JSON structure from main computer and mark them as required
    create_folders_from_json()

    # Step 2: Clean up extra folders by deleting only the latest empty sub-folder
    delete_latest_extra_folder(main_folder_path)

    print("Folder structure updated.")

if __name__ == "__main__":
    # Export to related git repository
    export_folder_structure(base_path=os.getenv("RC_RAW_FILE_PATH"),
                            output_file=os.getenv("RC_FOLDER_STRUCTURE_JSON"))