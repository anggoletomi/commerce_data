from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

import json

# A. SHOPEE
rc_shopee_store_info = {}

for i in range(1, 100):
    rc_env_var = f'RC_SHOPEE_STORE_INFO_{i}'
    rc_value = os.getenv(rc_env_var)
    
    if rc_value:
        rc_store_data = json.loads(rc_value)
        rc_shopee_store_info.update(rc_store_data)
    else:
        continue

rc_shopee_order_path = [f"rc_raw_file/shopee_order/{key}/" for key in rc_shopee_store_info]

rc_shopee_pay_path = [f"rc_raw_file/shopee_pay/{key}/" for key in rc_shopee_store_info]

rc_shopee_income_path = [f"rc_raw_file/shopee_income/{key}/" for key in rc_shopee_store_info]

# Google Drive Folder Mapping
def get_store_key(env_var):
    """Safely retrieve the key for the Shopee store from an environment variable."""
    value = os.getenv(env_var)
    if value:
        try:
            store_info = json.loads(value)
            if store_info:  # Ensure the parsed JSON contains keys
                return list(store_info.keys())[0]
        except json.JSONDecodeError:
            print(f"Warning: Invalid JSON format in environment variable '{env_var}'")
    return None  # Return None if the variable is missing or invalid

# Define the folder mapping with placeholders
folder_mapping = {
    "shopee_income": {
        "RC_SHOPEE_STORE_INFO_1": "1EkOwVnuKpupZpkJCPizSObQ_7pi8wI12",
        "RC_SHOPEE_STORE_INFO_2": "1uuYhYoil27NwZI9MVWkv0pfbAQ_OgqAv",
        "RC_SHOPEE_STORE_INFO_3": "1yV5ShZ6oyrE2z1kVI5WWoDREYNdb9pBL",
        "RC_SHOPEE_STORE_INFO_4": "1mefdL1aC4N3XFZMOE-5t-h9vdsR6Gh6I",
        "RC_SHOPEE_STORE_INFO_5": "17Ft8kky9rhYAwjV1M8ifNZyQHfnjO2Wu",
        "RC_SHOPEE_STORE_INFO_6": "11MdKfhYwNIqFDCBhxkW9L2kQvZlD4pov",
        "RC_SHOPEE_STORE_INFO_7": "1aoEcvHxkWlIBJmLj74iYkqIp5o_wLmSY",
        "RC_SHOPEE_STORE_INFO_8": "1tqqQCeol0_JQNHWEYSsgqTEaD64mQXOG",
    },
    "shopee_order": {
        "RC_SHOPEE_STORE_INFO_1": "1CWjll_IiSN9iWkVLuNf4KRBwJTCCTqXR",
        "RC_SHOPEE_STORE_INFO_2": "1qHfJToDKxPgiQ-DSGn1Mk6mbBkVnk6f8",
        "RC_SHOPEE_STORE_INFO_3": "1SXAwFsngXH31T37eudVlqvxaHbrdqoNe",
        "RC_SHOPEE_STORE_INFO_4": "1kwEj_guhF-6oWB3EMhMYWkKt_41PIHmL",
        "RC_SHOPEE_STORE_INFO_5": "1eN6fr7JT9o5hkQ_JVGFr9nTCikxoFV8Z",
        "RC_SHOPEE_STORE_INFO_6": "16GdAHJeg10TD-ofnuOh10kQK_MRmLCD5",
        "RC_SHOPEE_STORE_INFO_7": "1TBMqYd_hx3B1hxPVxNuFKsbe1qkDioCl",
        "RC_SHOPEE_STORE_INFO_8": "1kcdDK80u5y_DwGPw6YJgWaRjT9IwK1_G",
    },
    "shopee_pay": {
        "RC_SHOPEE_STORE_INFO_1": "1zp8OcrIMqMO80x8bb0D1qz79KoxbNUie",
        "RC_SHOPEE_STORE_INFO_2": "1rd-e7aEfI6Qjbacyr2Y2jnzVK0DYnDCd",
        "RC_SHOPEE_STORE_INFO_3": "14rsDO-oRu24FyhWgG4iAonwkJMshe2x9",
        "RC_SHOPEE_STORE_INFO_4": "1sEA5B5WbopdOzJCDUl2_9DP6aOZC1BNp",
        "RC_SHOPEE_STORE_INFO_5": "1F1Wav0F8bJQxluSAfVETvxvyRZzeXdl7",
        "RC_SHOPEE_STORE_INFO_6": "1KXmSiH9E8IrmB3AjD3JyJ-TXoBt13Xlp",
        "RC_SHOPEE_STORE_INFO_7": "1U00Gs0JReY72AET58Tce-FdhfHzKExma",
        "RC_SHOPEE_STORE_INFO_8": "1Sd0AJmNk2YQYqKTA9ijVp08eU6WinPP0",
    },
}

# Build the final dictionary dynamically
rc_shopee_gdrive_folder = {}
for folder_type, mapping in folder_mapping.items():
    for env_var, folder_id in mapping.items():
        store_key = get_store_key(env_var)
        if store_key:  # Only include if the environment variable is valid
            rc_shopee_gdrive_folder[f'rc_raw_file/{folder_type}/{store_key}'] = folder_id

rc_shopee_wallet_category_mappings = {
    'penggantian dana penuh barang hilang': {
        'simple': 'Penggantian Barang Hilang/Rusak',
        'english': 'Full Refund for Lost Item',
        'database': 'refund_lost_full'
    },
    'penggantian dana penuh barang rusak': {
        'simple': 'Penggantian Barang Hilang/Rusak',
        'english': 'Full Refund for Damaged Item',
        'database': 'refund_damaged_full'
    },
    'penggantian dana sebagian barang hilang': {
        'simple': 'Penggantian Barang Hilang/Rusak',
        'english': 'Partial Refund for Lost Item',
        'database': 'refund_lost_partial'
    },
    'penggantian dana sebagian barang rusak': {
        'simple': 'Penggantian Barang Hilang/Rusak',
        'english': 'Partial Refund for Damaged Item',
        'database': 'refund_damaged_partial'
    },
    'isi ulang saldo iklan': {
        'simple': 'Saldo Iklan',
        'english': 'Ads Balance Refill',
        'database': 'ads_balance_refill'
    },
    'kompensasi kehilangan': {
        'simple': 'Kompensasi Kehilangan',
        'english': 'Compensation for Loss',
        'database': 'loss_compensation'
    },
    'penarikan dana': {
        'simple': 'Penarikan Dana',
        'english': 'Fund Withdrawal',
        'database': 'withdrawal'
    },
    'penghasilan dari pesanan': {
        'simple': 'Penghasilan Dari Pesanan',
        'english': 'Income from Orders',
        'database': 'order_income'
    },
    'pemotongan biaya komisi ams': {
        'simple': 'Pemotongan Biaya Komisi AMS',
        'english': 'AMS Commission Deduction',
        'database': 'ams_commission_deduction'
    },
    'pengembalian dana untuk penarikan gagal': {
        'simple': 'Pengembalian Dana Gagal',
        'english': 'Refund for Failed Withdrawal',
        'database': 'refund_failed_withdrawal'
    },
    '[penambahan wallet] pelepasan dana': {
        'simple': 'Penambahan Wallet - Pelepasan Dana',
        'english': 'Wallet Addition - Fund Release',
        'database': 'wallet_addition_fund_release'
    },
    '[pengurangan wallet] pelepasan dana berlebih': {
        'simple': 'Pengurangan Wallet - Pelepasan Dana Berlebih',
        'english': 'Wallet Deduction - Excess Fund Release',
        'database': 'wallet_deduction_excess_fund_release'
    },
    'penambahan wallet campaign package': {
        'simple': 'Penambahan Wallet - Campaign Package',
        'english': 'Wallet Addition - Campaign Package',
        'database': 'wallet_addition_campaign_package'
    },
    'penyesuaian ongkos kirim bebas pengembalian': {
        'simple': 'Penyesuaian Ongkos Kirim - Bebas Pengembalian',
        'english': 'Shipping Adjustment - Free Returns',
        'database': 'shipping_adjustment_free_returns'
    },
    'csp x ams cashback': {
        'simple': 'CSP X AMS Cashback',
        'english': 'CSP X AMS Cashback',
        'database': 'csp_ams_cashback'
    },
    'penyesuaian untuk': {
        'simple': 'Penyesuaian Order',
        'english': 'Order Adjustment',
        'database': 'order_adjustment'
    },
    'penyesuaian saldo penjual karena kekurangan ongkir pengembalian': {
        'simple': 'Penyesuaian Saldo Penjual - Kurang Ongkir Pengembalian',
        'english': 'Seller Balance Adjustment for Return Shipping Shortfall',
        'database': 'seller_balance_adjustment_return_shipping'
    },
    'nominal program cashback pick up spx express': {
        'simple': 'Cashback Logistik',
        'english': 'Logistic Cashback',
        'database': 'logistic_cashback'
    },
    'auto-accept without judging': {
        'simple': 'Auto-accept Without Judging - Campaign',
        'english': 'Auto-accept Without Judging - Campaign',
        'database': 'auto_accept_without_judging'
    },
    'auto-approve compensation without judging': {
        'simple': 'Auto-approve Compensation Without Judging',
        'english': 'Auto-approve Compensation Without Judging',
        'database': 'auto_approve_compensation_without_judging'
    },
    'kompensasi biaya kemasan program garansi bebas pengembalian': {
        'simple': 'Kompensasi Biaya Kemasan Program Garansi Bebas Pengembalian',
        'english': 'Packaging Cost Compensation - Free Return Guarantee Program',
        'database': 'free_return_program_packaging_cost'
    },
    'pengembalian dana subsidi harga shopee': {
        'simple': 'Pengembalian Dana Subsidi Harga Shopee',
        'english': 'Refund of Shopee Price Subsidy',
        'database': 'shopee_price_subsidy_refund'
    },
    'strong seller evidence': {
        'simple': 'Strong Seller Evidence',
        'english': 'Strong Seller Evidence',
        'database': 'strong_seller_evidence'
    },
}
