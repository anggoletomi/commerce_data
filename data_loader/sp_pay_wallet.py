from dotenv import load_dotenv
load_dotenv()

import sys,os
sys.path.insert(0, os.getenv("PROJECT_PATH"))

from bi_function import *

def sp_pay_wallet(count_file,target_table,db_method,data_path,store_dim):

    file_path = get_latest_file_multiple_folder([os.path.join(os.getenv("BASE_RAW_FILE_PATH", ""), folder) for folder in data_path],n=count_file)

    print(f'count_file = {count_file}')
    print(f'actual file = {len(file_path)}')

    def read_table(path):

        try:
            df = pd.read_excel(path,header=17,dtype=str)
        except Exception as e:
            print(f'\033[1;31m--Failed to read the file: {path}. Error: {str(e)}\033[0m')
            return pd.DataFrame()
        
        column_mapping = {
            'tanggal transaksi': 'transaction_date',
            'tipe transaksi': 'transaction_type',
            'deskripsi': 'description',
            'no. pesanan': 'order_number',
            'jenis transaksi': 'transaction_category',
            'jumlah': 'amount',
            'status': 'status',
            'saldo akhir': 'ending_balance'
        }

        default_col = ['transaction_date', 'transaction_type', 'description',  'order_number', 'transaction_category',
                       'amount', 'status', 'ending_balance']

        df.columns = [column_mapping.get(col.lower(), col) for col in df.columns]

        try:
            df = df[default_col]
        except Exception as e:
            print(f'\033[1;31m--Column schema has changed: {path}. Error: {str(e)}\033[0m')
            return pd.DataFrame()

        # Clean Data Type

        try:

            for d in ['transaction_date']:
                df[d] = pd.to_datetime(df[d], errors='coerce')

            for f in ['amount','ending_balance']:
                df[f] = df[f].astype(float)

        except Exception as e:
            print(f'\033[1;31m--There is some issue when cleaning the data type: {path}. Error: {str(e)}\033[0m')
            return pd.DataFrame()
        
        # Map Dimension

        df.insert(0, 'month_wallet', df['transaction_date'].dt.strftime('%Y%m'))

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
                            unique_col_ref = ['folder_id','transaction_type','description','order_number','transaction_category'], # need to be optimized, still have duplicate with this combination when append
                            date_col_ref = 'transaction_date'
                            )