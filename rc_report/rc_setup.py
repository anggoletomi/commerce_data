# SHOPEE
shopee_store_info = {#'store_id','country','currency','platform','store'
                    'BEAUTYADDICT_490889707' : ['490889707','ID','IDR','SHOPEE','BEAUTY ADDICT'],
                    'BEAUTYENTHUSIAST_492800097' : ['492800097','ID','IDR','SHOPEE','BEAUTY ENTHUSIAST'],
                    'BEAUTYEXPERT_492808501' : ['492808501','ID','IDR','SHOPEE','BEAUTY EXPERT'],
                    'BEAUTYPEDIA_494291072' : ['494291072','ID','IDR','SHOPEE','BEAUTYPEDIA'],
                    'CORNERBEAUTY_43579390' : ['43579390','ID','IDR','SHOPEE','CORNER BEAUTY'],
                    'PAMELO_402110126' : ['402110126','ID','IDR','SHOPEE','PAMELO'],
                    'PAVIOLA_838599322' : ['838599322','ID','IDR','SHOPEE','PAVIOLA'],
                    'RASICO_691779308' : ['691779308','ID','IDR','SHOPEE','RASICO'],
                     } 

shopee_order_path = [f"rc_raw_file/shopee_order/{key}/" for key in shopee_store_info]

shopee_pay_path = [f"rc_raw_file/shopee_pay/{key}/" for key in shopee_store_info]

shopee_income_path = [f"rc_raw_file/shopee_income/{key}/" for key in shopee_store_info]

shopee_gdrive_folder = {
    'rc_raw_file/shopee_income/BEAUTYADDICT_490889707' : '1EkOwVnuKpupZpkJCPizSObQ_7pi8wI12',
    'rc_raw_file/shopee_income/BEAUTYENTHUSIAST_492800097' : '1uuYhYoil27NwZI9MVWkv0pfbAQ_OgqAv',
    'rc_raw_file/shopee_income/BEAUTYEXPERT_492808501' : '1yV5ShZ6oyrE2z1kVI5WWoDREYNdb9pBL',
    'rc_raw_file/shopee_income/BEAUTYPEDIA_494291072' : '1mefdL1aC4N3XFZMOE-5t-h9vdsR6Gh6I',
    'rc_raw_file/shopee_income/CORNERBEAUTY_43579390' : '17Ft8kky9rhYAwjV1M8ifNZyQHfnjO2Wu',
    'rc_raw_file/shopee_income/PAMELO_402110126' : '11MdKfhYwNIqFDCBhxkW9L2kQvZlD4pov',
    'rc_raw_file/shopee_income/PAVIOLA_838599322' : '1aoEcvHxkWlIBJmLj74iYkqIp5o_wLmSY',
    'rc_raw_file/shopee_income/RASICO_691779308' : '1tqqQCeol0_JQNHWEYSsgqTEaD64mQXOG',
    'rc_raw_file/shopee_order/BEAUTYADDICT_490889707' : '1CWjll_IiSN9iWkVLuNf4KRBwJTCCTqXR',
    'rc_raw_file/shopee_order/BEAUTYENTHUSIAST_492800097' : '1qHfJToDKxPgiQ-DSGn1Mk6mbBkVnk6f8',
    'rc_raw_file/shopee_order/BEAUTYEXPERT_492808501' : '1SXAwFsngXH31T37eudVlqvxaHbrdqoNe',
    'rc_raw_file/shopee_order/BEAUTYPEDIA_494291072' : '1kwEj_guhF-6oWB3EMhMYWkKt_41PIHmL',
    'rc_raw_file/shopee_order/CORNERBEAUTY_43579390' : '1eN6fr7JT9o5hkQ_JVGFr9nTCikxoFV8Z',
    'rc_raw_file/shopee_order/PAMELO_402110126' : '16GdAHJeg10TD-ofnuOh10kQK_MRmLCD5',
    'rc_raw_file/shopee_order/PAVIOLA_838599322' : '1TBMqYd_hx3B1hxPVxNuFKsbe1qkDioCl',
    'rc_raw_file/shopee_order/RASICO_691779308' : '1kcdDK80u5y_DwGPw6YJgWaRjT9IwK1_G',
    'rc_raw_file/shopee_pay/BEAUTYADDICT_490889707' : '1zp8OcrIMqMO80x8bb0D1qz79KoxbNUie',
    'rc_raw_file/shopee_pay/BEAUTYENTHUSIAST_492800097' : '1rd-e7aEfI6Qjbacyr2Y2jnzVK0DYnDCd',
    'rc_raw_file/shopee_pay/BEAUTYEXPERT_492808501' : '14rsDO-oRu24FyhWgG4iAonwkJMshe2x9',
    'rc_raw_file/shopee_pay/BEAUTYPEDIA_494291072' : '1sEA5B5WbopdOzJCDUl2_9DP6aOZC1BNp',
    'rc_raw_file/shopee_pay/CORNERBEAUTY_43579390' : '1F1Wav0F8bJQxluSAfVETvxvyRZzeXdl7',
    'rc_raw_file/shopee_pay/PAMELO_402110126' : '1KXmSiH9E8IrmB3AjD3JyJ-TXoBt13Xlp',
    'rc_raw_file/shopee_pay/PAVIOLA_838599322' : '1U00Gs0JReY72AET58Tce-FdhfHzKExma',
    'rc_raw_file/shopee_pay/RASICO_691779308' : '1Sd0AJmNk2YQYqKTA9ijVp08eU6WinPP0',
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
}