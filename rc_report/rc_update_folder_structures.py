from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_folder_structures import update_folder_structure

update_folder_structure(target_path=os.getenv("RC_RAW_FILE_PATH"),
                        structure_file=os.getenv("RC_FOLDER_STRUCTURE_JSON"))