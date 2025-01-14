from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_function import *

def sp_order_data(count_file,target_table,db_method,data_path,store_dim):

    file_path = get_latest_file_multiple_folder([os.path.join(os.getenv("BASE_RAW_FILE_PATH", ""), folder) for folder in data_path],n=count_file)

    print(f'count_file = {count_file}')
    print(f'actual file = {len(file_path)}')

    def read_table(path):

        try:
            df = pd.read_excel(path,dtype=str)
        except Exception as e:
            print(f'\033[1;31m--Failed to read the file: {path}. Error: {str(e)}\033[0m')
            return pd.DataFrame()

        column_mapping = {
            'no. pesanan': 'order_number',
            'status pesanan': 'order_status',
            'alasan pembatalan': 'cancellation_reason',
            'status pembatalan/ pengembalian': 'cancellation_return_status',
            'no. resi': 'tracking_number',
            'opsi pengiriman': 'shipping_option',
            'antar ke counter/ pick-up': 'counter_pickup_option',
            'pesanan harus dikirimkan sebelum (menghindari keterlambatan)': 'order_delivery_deadline',
            'waktu pengiriman diatur': 'scheduled_delivery_time',
            'waktu pesanan dibuat': 'order_creation_time',
            'waktu pembayaran dilakukan': 'payment_time',
            'metode pembayaran': 'payment_method',
            'sku induk': 'parent_sku',
            'nama produk': 'product_name',
            'nomor referensi sku': 'sku_reference_number',
            'nama variasi': 'variant_name',
            'harga awal': 'initial_price',
            'harga setelah diskon': 'price_after_discount',
            'jumlah': 'quantity',
            'returned quantity': 'returned_quantity',
            'total harga produk': 'total_product_price',
            'total diskon': 'total_discount',
            'diskon dari penjual': 'seller_discount',
            'diskon dari shopee': 'shopee_discount',
            'berat produk': 'product_weight',
            'jumlah produk di pesan': 'ordered_product_quantity',
            'total berat': 'total_weight',
            'voucher ditanggung penjual': 'seller_borne_voucher',
            'cashback koin': 'coin_cashback',
            'voucher ditanggung shopee': 'shopee_borne_voucher',
            'paket diskon': 'discount_package',
            'paket diskon (diskon dari shopee)': 'shopee_discount_package',
            'paket diskon (diskon dari penjual)': 'seller_discount_package',
            'potongan koin shopee': 'shopee_coin_deduction',
            'diskon kartu kredit': 'credit_card_discount',
            'ongkos kirim dibayar oleh pembeli': 'buyer_paid_shipping_cost',
            'estimasi potongan biaya pengiriman': 'estimated_shipping_deduction',
            'ongkos kirim pengembalian barang': 'return_shipping_cost',
            'total pembayaran': 'total_payment',
            'perkiraan ongkos kirim': 'estimated_shipping_cost',
            'catatan dari pembeli': 'buyer_note',
            'catatan': 'note',
            'username (pembeli)': 'buyer_username',
            'nama penerima': 'recipient_name',
            'no. telepon': 'phone_number',
            'alamat pengiriman': 'shipping_address',
            'kota/kabupaten': 'city_district',
            'provinsi': 'province',
            'waktu pesanan selesai': 'order_completion_time'
        }

        default_col = ['order_number', 'order_status', 'cancellation_reason',  'cancellation_return_status', 'tracking_number',
                    'shipping_option',  'counter_pickup_option', 'order_delivery_deadline', 'scheduled_delivery_time',
                    'order_creation_time', 'payment_time', 'payment_method', 'parent_sku',  'product_name',
                    'sku_reference_number', 'variant_name', 'initial_price',  'price_after_discount', 'quantity',
                    'returned_quantity', 'total_product_price',  'total_discount', 'seller_discount', 'shopee_discount',
                    'product_weight',  'ordered_product_quantity', 'total_weight', 'seller_borne_voucher', 'coin_cashback',
                    'shopee_borne_voucher', 'discount_package', 'shopee_discount_package',  'seller_discount_package',
                    'shopee_coin_deduction', 'credit_card_discount',  'buyer_paid_shipping_cost', 'estimated_shipping_deduction',
                    'return_shipping_cost',  'total_payment', 'estimated_shipping_cost', 'buyer_note', 'note',
                    'buyer_username', 'recipient_name', 'phone_number', 'shipping_address',  'city_district',
                    'province', 'order_completion_time']

        df.columns = [column_mapping.get(col.lower(), col) for col in df.columns]

        try:
            df = df[default_col]
        except Exception as e:
            print(f'\033[1;31m--Column schema has changed: {path}. Error: {str(e)}\033[0m')
            return pd.DataFrame()

        # Clean Data Type

        try:

            for d in ['order_delivery_deadline','scheduled_delivery_time','order_creation_time','payment_time','order_completion_time']:
                df[d] = pd.to_datetime(df[d], errors='coerce')

            for i in ['quantity','returned_quantity','ordered_product_quantity']:
                df[i] = df[i].astype('int64')

            for f in ['initial_price','price_after_discount','total_product_price','total_discount','seller_discount',
                    'shopee_discount','seller_borne_voucher','coin_cashback','shopee_borne_voucher','shopee_discount_package',
                    'seller_discount_package','shopee_coin_deduction','credit_card_discount','buyer_paid_shipping_cost',
                    'estimated_shipping_deduction','return_shipping_cost','total_payment','estimated_shipping_cost']:
                df[f] = df[f].str.replace('.','')
                df[f] = df[f].replace('err', np.nan)
                df[f] = df[f].astype(float)

        except Exception as e:
            print(f'\033[1;31m--There is some issue when cleaning the data type: {path}. Error: {str(e)}\033[0m')
            return pd.DataFrame()
        
        # Map Dimension

        df.insert(0, 'month_order', df['order_creation_time'].dt.strftime('%Y%m'))

        uq_id = Path(path).parts[-2]

        df.insert(1, 'store_id', store_dim[uq_id][0])
        df.insert(2, 'country', store_dim[uq_id][1])
        df.insert(3, 'currency', store_dim[uq_id][2])
        df.insert(4, 'platform', store_dim[uq_id][3])
        df.insert(5, 'store', store_dim[uq_id][4])
        df.insert(6, 'folder_id', uq_id)
        
        return df

    df = pd.concat([read_table(path) for path in file_path], ignore_index=True)
    
    write_table_by_unique_id(df,
                            target_table = target_table,
                            write_method=db_method,
                            unique_col_ref = ['folder_id','order_number'],
                            date_col_ref = 'order_creation_time'
                            )