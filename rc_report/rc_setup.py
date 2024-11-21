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