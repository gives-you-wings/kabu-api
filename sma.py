from dotenv import load_dotenv
import requests
import db_connection
import my_logger
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()


def main():
    select_sql = '''select stock_code from stock.tbl_m_stock_info
                where tbl_m_stock_info.stock_code > '43720' order by stock_code'''
    stock_codes = connection.select_all(select_sql)

    with ThreadPoolExecutor(max_workers=2) as executor:
        for stock_code in stock_codes:
            executor.submit(calculate_sma, stock_code[0], 5)
            executor.submit(calculate_sma, stock_code[0], 25)


def calculate_sma(stock_code, window):
    logger.info('stock_code: %s' % stock_code)
    select_sql = '''select date, adjustment_close from stock.tbl_t_daily_price where stock_code = %s
                    order by date '''
    rows = connection.select_all(select_sql, (stock_code,))
    logger.info('count: %s' % len(rows))
    # logger.info('rows: %s' % rows)
    df = pd.DataFrame(rows, columns=['date', 'adjustment_close'])
    # date列を日付型に変換
    df['date'] = pd.to_datetime(df['date'])

    # date列をインデックスに設定
    df.set_index('date', inplace=True)
    # 欠損値を補完
    df['adjustment_close'] = df['adjustment_close'].interpolate()
    # 移動平均を計算（ここではwindow日間の移動平均）
    df['sma'] = df['adjustment_close'].rolling(window=window).mean()

    # logger.info(df)
    if window == 5:
        update_sql = '''UPDATE stock.tbl_t_daily_price SET sma_5 = %s WHERE stock_code = %s and date = %s'''
    elif window == 25:
        update_sql = '''UPDATE stock.tbl_t_daily_price SET sma_25 = %s WHERE stock_code = %s and date = %s'''
    # DBに保存
    for index, row in df.iterrows():
        # logger.info('date: %s sma: %s' % (index, row['sma']))
        connection.execute(update_sql, (row['sma'], stock_code, index))
        logger.debug('date: %s sma: %s updated.' % (index, row['sma']))


main()
