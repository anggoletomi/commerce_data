from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_function import log_function

from rc_gdrive_data_sync import main_drive_to_local
from sp_income_released import sp_income_released
from sp_order_data import sp_order_data
from sp_pay_wallet import sp_pay_wallet


tasks = [
    (main_drive_to_local, {}),

    (sp_income_released, {'count_file': 1000,'db_method': 'replace'}),
    (sp_order_data, {'count_file': 1000,'db_method': 'replace'}),
    (sp_pay_wallet, {'count_file': 1000,'db_method': 'replace'}),
]

log_function(tasks)