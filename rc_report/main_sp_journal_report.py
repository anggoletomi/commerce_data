from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_function import log_function

from rc_gdrive_data_sync import main_drive_to_local
from rp_sp_journal_report import *
from sp_income_released import sp_income_released
from sp_order_data import sp_order_data
from sp_pay_wallet import sp_pay_wallet


tasks = [
    (main_drive_to_local, {}),

    (sp_income_released, {'count_file': 1000,'db_method': 'replace'}),
    (sp_order_data, {'count_file': 1000,'db_method': 'replace'}),
    (sp_pay_wallet, {'count_file': 1000,'db_method': 'replace'}),

    (create_journal_base, {'journal_base': False, 'start_date': '2024-01-01', 'db_method': 'replace', 'transform' : False}), # 1. Create Journal Order

    (create_journal_base, {'journal_base': False, 'start_date': '2024-01-01', 'db_method': 'replace', 'transform' : True}), # 2. Create Journal Order Transform
]

months = get_month_list(month_start='202401',month_end='202412',month_format='%Y%m')

# 3. Create Journal Base (looped)
for folder in shopee_store_info.keys():
    for month in months:
        tasks.append((create_journal_base, {'journal_base': True, 'data_month': month, 'folder_id': folder, 'db_method': 'append', 'transform' : False}))

# 4. Create Journal Dashboard (looped)
for folder in shopee_store_info.keys():
    for month in months:
        tasks.append((create_journal_dashboard, {'report_month': month, 'folder_id': folder, 'db_method': 'append'}))

log_function(tasks)