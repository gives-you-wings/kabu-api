import json

import yfinance
from dotenv import load_dotenv
import requests
import db_connection
import my_logger

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()

select_sql = '''select stock_code from stock.tbl_m_stock_info where stock_code = '82670' order by stock_code'''
rows = connection.select_all(select_sql)
logger.info(rows)
for row in rows:
    target_code = row['stock_code'][:-1] + '.T'
    logger.info(target_code)

    stock_code = row['stock_code']
    result = yfinance.Ticker(target_code)
    print(result.recommendations_summary)
    # for idx in result.recommendations.index:
    #     logger.info(idx)
    #     logger.info(result.dividends[idx])
    #     upsert_sql = '''INSERT INTO stock.tbl_t_dividend_history(stock_code, dividend_date, dividend_amount, created_at)
    #     values (%s, %s, %s, now())
    #     on conflict(stock_code, dividend_date) do update set dividend_amount = %s, updated_at = now()'''
    #     connection.execute(upsert_sql, (stock_code, idx, result.dividends[idx], result.dividends[idx]))
    #     logger.info('insert or update stock info done. ')
