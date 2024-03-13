import json

import yfinance
from dotenv import load_dotenv
import requests
import db_connection
import my_logger

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()

select_sql = '''select m.stock_code, m.company_name
                    from trade.stock.tbl_m_stock_info m
                    left join trade.stock.tbl_t_statement s on m.stock_code = s.stock_code
                    where m.sector_17_code <> '99'
                    and m.stock_code <> '49870'
                    and m.stock_code not like '%A%'
                    order by m.stock_code'''
# select_sql = '''select stock_code from stock.tbl_m_stock_info where stock_code = '22940' order by stock_code'''
rows = connection.select_all(select_sql)
logger.info(rows)
for row in rows:
    target_code = row['stock_code'][:-1] + '.T'
    logger.info(target_code)

    stock_code = row['stock_code']
    result = yfinance.Ticker(target_code)
    logger.info(result.calendar)

    upsert_sql = '''INSERT INTO stock.tbl_t_calendar(stock_code, ex_dividend_date, earnings_date_from, earnings_date_to, created_at)
    values (%s, %s, %s, %s, now())
    on conflict(stock_code) do update set ex_dividend_date = %s, earnings_date_from = %s, earnings_date_to = %s, updated_at = now()'''

    if 'Ex-Dividend Date' in result.calendar:
        exDividendDate = result.calendar['Ex-Dividend Date']
    else:
        exDividendDate = None
    earningsDate = result.calendar['Earnings Date']
    earningsDateFrom = earningsDate[0] if len(earningsDate) > 0 else None
    if len(earningsDate) > 1:
        earningsDateTo = earningsDate[1]
    else:
        earningsDateTo = None

    # logger.info(exDividendDate)
    # logger.info(earningsDateFrom)
    # logger.info(earningsDateTo)

    connection.execute(upsert_sql, (
        stock_code, exDividendDate, earningsDateFrom, earningsDateTo, exDividendDate, earningsDateFrom, earningsDateTo))
    logger.info('insert or update stock info done. ')
