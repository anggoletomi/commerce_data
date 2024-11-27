import os
import json
from dotenv import load_dotenv
load_dotenv()

# A. SHOPEE
shopee_store_info = {}

# Iterate through possible keys
for i in range(1, 100):
    env_var = f'RC_SHOPEE_STORE_INFO_{i}'
    value = os.getenv(env_var)
    
    if value:
        store_data = json.loads(value)
        shopee_store_info.update(store_data)
    else:
        break

shopee_order_path = [f"rc_raw_file/shopee_order/{key}/" for key in shopee_store_info]

shopee_pay_path = [f"rc_raw_file/shopee_pay/{key}/" for key in shopee_store_info]

shopee_income_path = [f"rc_raw_file/shopee_income/{key}/" for key in shopee_store_info]

shopee_gdrive_folder = {
    f'rc_raw_file/shopee_income/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_1")).keys())[0]}' : '1EkOwVnuKpupZpkJCPizSObQ_7pi8wI12',
    f'rc_raw_file/shopee_income/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_2")).keys())[0]}' : '1uuYhYoil27NwZI9MVWkv0pfbAQ_OgqAv',
    f'rc_raw_file/shopee_income/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_3")).keys())[0]}' : '1yV5ShZ6oyrE2z1kVI5WWoDREYNdb9pBL',
    f'rc_raw_file/shopee_income/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_4")).keys())[0]}' : '1mefdL1aC4N3XFZMOE-5t-h9vdsR6Gh6I',
    f'rc_raw_file/shopee_income/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_5")).keys())[0]}' : '17Ft8kky9rhYAwjV1M8ifNZyQHfnjO2Wu',
    f'rc_raw_file/shopee_income/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_6")).keys())[0]}' : '11MdKfhYwNIqFDCBhxkW9L2kQvZlD4pov',
    f'rc_raw_file/shopee_income/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_7")).keys())[0]}' : '1aoEcvHxkWlIBJmLj74iYkqIp5o_wLmSY',
    f'rc_raw_file/shopee_income/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_8")).keys())[0]}' : '1tqqQCeol0_JQNHWEYSsgqTEaD64mQXOG',
    
    f'rc_raw_file/shopee_order/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_1")).keys())[0]}' : '1CWjll_IiSN9iWkVLuNf4KRBwJTCCTqXR',
    f'rc_raw_file/shopee_order/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_2")).keys())[0]}' : '1qHfJToDKxPgiQ-DSGn1Mk6mbBkVnk6f8',
    f'rc_raw_file/shopee_order/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_3")).keys())[0]}' : '1SXAwFsngXH31T37eudVlqvxaHbrdqoNe',
    f'rc_raw_file/shopee_order/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_4")).keys())[0]}' : '1kwEj_guhF-6oWB3EMhMYWkKt_41PIHmL',
    f'rc_raw_file/shopee_order/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_5")).keys())[0]}' : '1eN6fr7JT9o5hkQ_JVGFr9nTCikxoFV8Z',
    f'rc_raw_file/shopee_order/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_6")).keys())[0]}' : '16GdAHJeg10TD-ofnuOh10kQK_MRmLCD5',
    f'rc_raw_file/shopee_order/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_7")).keys())[0]}' : '1TBMqYd_hx3B1hxPVxNuFKsbe1qkDioCl',
    f'rc_raw_file/shopee_order/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_8")).keys())[0]}' : '1kcdDK80u5y_DwGPw6YJgWaRjT9IwK1_G',
    
    f'rc_raw_file/shopee_pay/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_1")).keys())[0]}' : '1zp8OcrIMqMO80x8bb0D1qz79KoxbNUie',
    f'rc_raw_file/shopee_pay/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_2")).keys())[0]}' : '1rd-e7aEfI6Qjbacyr2Y2jnzVK0DYnDCd',
    f'rc_raw_file/shopee_pay/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_3")).keys())[0]}' : '14rsDO-oRu24FyhWgG4iAonwkJMshe2x9',
    f'rc_raw_file/shopee_pay/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_4")).keys())[0]}' : '1sEA5B5WbopdOzJCDUl2_9DP6aOZC1BNp',
    f'rc_raw_file/shopee_pay/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_5")).keys())[0]}' : '1F1Wav0F8bJQxluSAfVETvxvyRZzeXdl7',
    f'rc_raw_file/shopee_pay/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_6")).keys())[0]}' : '1KXmSiH9E8IrmB3AjD3JyJ-TXoBt13Xlp',
    f'rc_raw_file/shopee_pay/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_7")).keys())[0]}' : '1U00Gs0JReY72AET58Tce-FdhfHzKExma',
    f'rc_raw_file/shopee_pay/{list(json.loads(os.getenv("RC_SHOPEE_STORE_INFO_8")).keys())[0]}' : '1Sd0AJmNk2YQYqKTA9ijVp08eU6WinPP0',
}

shopee_wallet_category_mappings = {
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