from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_function import *

def sp_income_released(count_file,target_table,db_method,data_path,store_dim):

    file_path = get_latest_file_multiple_folder([os.path.join(os.getenv("BASE_RAW_FILE_PATH", ""), folder) for folder in data_path],n=count_file)

    print(f'count_file = {count_file}')
    print(f'actual file = {len(file_path)}')

    def read_table(path):

        possible_sheet_names = ['Income','Income - 1','Income - 2','Income - 3','Income - 4','Income - 5',
                                'Income - 6','Income - 7','Income - 8','Income - 9','Income - 10']
        data_frames = []

        for sheet_name in possible_sheet_names:
            try:
                data = pd.read_excel(path, sheet_name=sheet_name, header=5, dtype=str)
                data_frames.append(data)
            except Exception:
                pass

        if not data_frames:
            print(f"\033[1;31m--Failed to read any valid sheets in file: {path}\033[0m")
            return pd.DataFrame()
        
        df = pd.concat(data_frames, ignore_index=True)
        
        column_mapping = {
        'no.': 'index',
        'no. pesanan': 'order_number',
        'no. pengajuan': 'submission_number',
        'username (pembeli)': 'buyer_username',
        'waktu pesanan dibuat': 'order_creation_time',
        'metode pembayaran pembeli': 'buyer_payment_method',
        'tanggal dana dilepaskan': 'fund_release_date',
        'harga asli produk': 'original_product_price',
        'total diskon produk': 'total_product_discount',
        'jumlah pengembalian dana ke pembeli': 'buyer_refund_amount',
        'diskon produk dari shopee': 'shopee_product_discount',
        'diskon voucher ditanggung penjual': 'seller_borne_voucher_discount',
        'cashback koin yang ditanggung penjual': 'seller_borne_cashback_coins',
        'ongkir dibayar pembeli': 'shipping_paid_by_buyer',
        'diskon ongkir ditanggung jasa kirim': 'shipping_discount_borne_by_courier',
        'gratis ongkir dari shopee': 'shopee_free_shipping',
        'ongkir yang diteruskan oleh shopee ke jasa kirim': 'shipping_fees_forwarded_to_courier',
        'ongkos kirim pengembalian barang': 'return_shipping_cost',
        'pengembalian biaya kirim': 'shipping_fee_refund',
        'biaya komisi ams': 'ams_commission_fee',
        'biaya administrasi': 'administration_fee',
        'biaya layanan (termasuk ppn 11%)': 'service_fee_incl_vat_11_percent',
        'premi': 'premium_fee',
        'biaya program': 'program_fee',
        'biaya kartu kredit': 'credit_card_fee',
        'biaya kampanye': 'campaign_fee',
        'bea masuk, ppn & pph': 'import_vat_income_tax',
        'total penghasilan': 'total_income',
        'kode voucher': 'voucher_code',
        'kompensasi': 'compensation',
        'promo gratis ongkir dari penjual': 'seller_free_shipping_promo',
        'jasa kirim': 'courier_service',
        'nama kurir': 'courier_name',
        'unnamed: 33': 'unnamed_column_33',
        'pengembalian dana ke pembeli': 'refund_to_buyer',
        'pro-rata koin yang ditukarkan untuk pengembalian barang': 'pro_rata_coin_refund_for_return',
        'pro-rata voucher shopee untuk pengembalian barang': 'pro_rata_shopee_voucher_for_return',
        'pro-rated bank payment channel promotion  for return refund items': 'pro_rated_bank_promo_for_return',
        'pro-rated shopee payment channel promotion  for return refund items': 'pro_rated_shopee_promo_for_return'
    }
        
        default_col = ['index','order_number','submission_number','buyer_username','order_creation_time',
                    'buyer_payment_method','fund_release_date','original_product_price','total_product_discount',
                    'buyer_refund_amount','shopee_product_discount','seller_borne_voucher_discount',
                    'seller_borne_cashback_coins','shipping_paid_by_buyer','shipping_discount_borne_by_courier',
                    'shopee_free_shipping','shipping_fees_forwarded_to_courier','return_shipping_cost',
                    'shipping_fee_refund','ams_commission_fee','administration_fee','service_fee_incl_vat_11_percent',
                    'premium_fee','program_fee','credit_card_fee','campaign_fee','import_vat_income_tax',
                    'total_income','voucher_code','compensation','seller_free_shipping_promo','courier_service',
                    'courier_name','unnamed_column_33','refund_to_buyer','pro_rata_coin_refund_for_return',
                    'pro_rata_shopee_voucher_for_return','pro_rated_bank_promo_for_return','pro_rated_shopee_promo_for_return']


        df.columns = [column_mapping.get(col.lower(), col) for col in df.columns]

        try:
            df = df[default_col]
        except Exception as e:
            print(f'\033[1;31m--Column schema has changed: {path}. Error: {str(e)}\033[0m')
            return pd.DataFrame()

        # Clean Data Type

        try:

            for d in ['order_creation_time','fund_release_date']:
                df[d] = pd.to_datetime(df[d], errors='coerce')

            for f in ['original_product_price','total_product_discount',
                    'buyer_refund_amount','shopee_product_discount','seller_borne_voucher_discount',
                    'seller_borne_cashback_coins','shipping_paid_by_buyer','shipping_discount_borne_by_courier',
                    'shopee_free_shipping','shipping_fees_forwarded_to_courier','return_shipping_cost',
                    'shipping_fee_refund','ams_commission_fee','administration_fee','service_fee_incl_vat_11_percent',
                    'premium_fee','program_fee','credit_card_fee','campaign_fee','import_vat_income_tax',
                    'total_income','compensation','seller_free_shipping_promo','refund_to_buyer','pro_rata_coin_refund_for_return',
                    'pro_rata_shopee_voucher_for_return','pro_rated_bank_promo_for_return','pro_rated_shopee_promo_for_return']:
                df[f] = df[f].str.replace('.','')
                df[f] = df[f].astype(float)

        except Exception as e:
            print(f'\033[1;31m--There is some issue when cleaning the data type: {path}. Error: {str(e)}\033[0m')
            return pd.DataFrame()
        
        # Map Dimension

        df.insert(0, 'month_income', df['fund_release_date'].dt.strftime('%Y%m'))
        df.insert(1, 'month_order', df['order_creation_time'].dt.strftime('%Y%m'))

        uq_id = Path(path).parts[-2]

        df.insert(2, 'store_id', store_dim[uq_id][0])
        df.insert(3, 'country', store_dim[uq_id][1])
        df.insert(4, 'currency', store_dim[uq_id][2])
        df.insert(5, 'platform', store_dim[uq_id][3])
        df.insert(6, 'store', store_dim[uq_id][4])
        df.insert(7, 'folder_id', uq_id)

        return df
        
    df = pd.concat([read_table(path) for path in file_path], ignore_index=True)
    
    write_table_by_unique_id(df,
                            target_table = target_table,
                            write_method=db_method,
                            unique_col_ref = ['folder_id','order_number'],
                            date_col_ref = 'fund_release_date'
                            )