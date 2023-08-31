import pandas as pd
import numpy as np
import re
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import os
import sys

# .env ファイルのキーバリューを環境変数に展開
from dotenv import load_dotenv
load_dotenv()

DATA_TYPE_MONTHLY = 'monthly'
DATA_TYPE_DAILY = 'daily'

args = sys.argv
if args[1] not in (DATA_TYPE_MONTHLY, DATA_TYPE_DAILY):
    print("invalid argument")
    sys.exit(1)

DATA_TYPE = args[1]

CN_DATE = 'date' # 日付
CN_MILK_COUNT = 'milk_count' # ミルク回数
CN_MILK_ML = 'milk_ml' # ミルク量
CN_UNCHI_COUNT = 'unchi_count' # 排便回数
CN_UNCHI_AMOUNT = 'unchi_amount' # 排便量
CN_AGE_OF_MONTH = 'age_of_month' # 月齢


# 対象データをリスト化する関数
def get_piyolog_all_items(all_text_data):

    # 対象項目
    def _check_item(text):
        if re.findall('ミルク|うんち', text) and re.match(r'([01][0-9]|2[0-3]):[0-5][0-9]', text):
            return True
        return False
    
    all_items = []

    for text_data in all_text_data:
        # 改行で分割
        lines = text_data.splitlines()
        array = np.array(lines)

        day = ''
        for index, item in enumerate(array):

            # 日付取得（月次データ）
            if DATA_TYPE == DATA_TYPE_MONTHLY and item == '----------' and index < len(array) - 1:
                day = array[index + 1][:-3] # 曜日「（月）など」の末尾3文字を除く文字列を抽出
                day_date = datetime.datetime.strptime(day, '%Y/%m/%d')
            # 日付取得（日次データ）
            elif DATA_TYPE == DATA_TYPE_DAILY and index == 0:
                day = array[index][6:-3] # 【ぴよログ】の先頭6文字と曜日「（月）など」の末尾3文字を除く文字列を抽出
                day_date = datetime.datetime.strptime(day, '%Y/%m/%d')


            # 対象項目の場合
            if item != '' and _check_item(item):
                # 空白で分割
                record = item.split()

                record_dt = datetime.datetime.strptime(day + ' ' + record[0], '%Y/%m/%d %H:%M')
                record_type = None
                record_subtype = record[1]
                record_value = None

                if 'ミルク' in record_subtype:
                    record_type = '食事'
                    # ミルク量
                    record_value = int(record[2].replace('ml', ''))

                if 'うんち' in record_subtype:
                    record_type = '排便'
                    # うんち量
                    if len(record) == 2: # 普通
                        record_value = 3
                    elif re.match(r'\(多め',record[2]): # 多め
                        record_value = 4
                    elif re.match(r'\(少なめ',record[2]): # 少なめ
                        record_value = 2
                    elif re.match(r'\(ちょこっと',record[2]): # ちょこっと
                        record_value = 1
                    else: # 普通
                        record_value = 3
                            
                # 記録
                all_items.append([day_date, record_dt, record_type, record_subtype, record_value])

    return all_items

def insert_to_db(df: pd.DataFrame):
    # dfからpostgresqlにインサート
    from sqlalchemy.engine.url import URL
    from sqlalchemy.engine.create import create_engine
    from sqlalchemy.exc import IntegrityError

    # DB の接続情報
    url = URL.create(
        drivername='mysql+mysqlconnector',
        username=os.getenv('USERNAME'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        port=3306,
        database=os.getenv('DATABASE'),
    )
    ssl_args = {'ssl_ca': './certificates/planetscale/cert.pem'}

    # DB に接続
    engine = create_engine(url, connect_args=ssl_args)

    # DB の stats テーブルに INSERT
    # primary key(日付)が重複していたら無視する
    for i in range(len(df)):
        try:
            df.iloc[i:i+1].to_sql('stats', con=engine, schema=None, if_exists='append', index=False)
        except IntegrityError:
            pass

def main():
    # 誕生日
    birth_date = datetime.datetime.strptime(os.getenv('BABY_BIRTH_DATE'), '%Y-%m-%d')

    # データ読込
    path = ''
    if DATA_TYPE == DATA_TYPE_MONTHLY:
        path = './data/monthly'
    elif DATA_TYPE == DATA_TYPE_DAILY:
        path =  './data/daily'

    # 指定したディレクトリ下の .txt ファイル名一覧を取得
    files = [_ for _ in os.listdir(path) if _.endswith(r'.txt')]
    all_text_data = []
    for filename in files:
        f = open(f'{path}/{filename}', encoding='utf-8')
        data = f.read()
        all_text_data.append(data)
        f.close()

    df = pd.DataFrame(get_piyolog_all_items(all_text_data),columns=[CN_DATE, '日時','分類','項目','量'])

    # 月齢
    month_age_list = []
    for i in range(0,10):
        month_age_list.append([birth_date + relativedelta(months=i+1),i])
        
    def _replace_month_age(x):
        for month_age in month_age_list:
            if x.date < month_age[0]:
                return month_age[1]
            
    df[CN_AGE_OF_MONTH] = df.apply(lambda x:_replace_month_age(x),axis=1)

    # 1日のミルク回数
    df_milk = df.query('項目=="ミルク"').groupby(CN_DATE).agg({'日時':'count', '量':'sum'}).reset_index()
    df_milk.columns = [CN_DATE,CN_MILK_COUNT,CN_MILK_ML]

    # 1日のうんち回数
    df_unchi = df.query('項目=="うんち"').groupby(CN_DATE).agg({'日時':'count', '量':'sum'}).reset_index()
    df_unchi.columns = [CN_DATE,CN_UNCHI_COUNT,CN_UNCHI_AMOUNT]

    # 重複した日付を削除して全日付を取得
    df = df[CN_DATE].drop_duplicates()

    # 1日集計のdf結合
    df_groupby_day = pd.merge(df, df_milk, on=CN_DATE, how='left')
    df_groupby_day = pd.merge(df_groupby_day, df_unchi, on=CN_DATE, how='left')

    # 月齢列の追加
    df_groupby_day[CN_AGE_OF_MONTH] = df_groupby_day.apply(lambda x:_replace_month_age(x),axis=1)

    insert_to_db(df_groupby_day)

if __name__ == "__main__":
    main()