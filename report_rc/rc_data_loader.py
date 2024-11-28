from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_function import log_function
from bi_folder_structures import update_folder_structure
from rc_setup import *
from rc_gdrive_data_sync import *

from data_loader.sp_income_released import sp_income_released
from data_loader.sp_order_data import sp_order_data
from data_loader.sp_pay_wallet import sp_pay_wallet

tasks = [

    (update_folder_structure, {'target_path': os.getenv("RC_RAW_FILE_PATH"),
                               'structure_file' : os.getenv("RC_FOLDER_STRUCTURE_JSON")}),

    (main_local_to_drive, {}),
    (main_drive_to_local, {}),
    
    (sp_income_released, {'count_file': 1000,
                          'target_table': 'report_rc.sp_income_released',
                          'db_method': 'replace',
                          'data_path' : rc_shopee_income_path,
                          'store_dim' : rc_shopee_store_info}),

    (sp_order_data, {'count_file': 1000,
                     'target_table': 'report_rc.sp_order_data',
                     'db_method': 'replace',
                     'data_path' : rc_shopee_order_path,
                     'store_dim' : rc_shopee_store_info}),

    (sp_pay_wallet, {'count_file': 1000,
                     'target_table': 'report_rc.sp_pay_wallet',
                     'db_method': 'replace',
                     'data_path' : rc_shopee_pay_path,
                     'store_dim' : rc_shopee_store_info}),

]

log_function(tasks)