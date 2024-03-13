import json

import yfinance
from dotenv import load_dotenv
import requests
import db_connection
import my_logger

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()

select_sql = '''select m.stock_code
                , m.company_name
                , dp.adjustment_close
                , s.forecast_dividend_per_share_annual
                , s.dividend_yield 
                , s.per
                , s.pbr
                , s.roe
                , c.ex_dividend_date
                , c.earnings_date_from
                from stock.tbl_t_statement s 
                inner join stock.tbl_m_stock_info m on s.stock_code = m.stock_code
                inner join stock.tbl_t_calendar c on s.stock_code = c.stock_code
                inner join stock.tbl_t_daily_price dp on s.stock_code = dp.stock_code
                and dp."date" =  current_date 
                where s.forecast_dividend_per_share_annual is not null
                and s.dividend_yield is not null
                and s.dividend_yield > 3
                --and c.earnings_date_from between '2024-03-13' and '2024-03-31'
                --order by c.earnings_date_from 
                order by s.dividend_yield desc '''
# select_sql = '''select stock_code from stock.tbl_m_stock_info where stock_code = '60910' order by stock_code'''
rows = connection.select_all(select_sql)
logger.info(rows)
for row in rows:
    target_code = row['stock_code'][:-1] + '.T'
    logger.info(target_code)

    stock_code = row['stock_code']
    result = yfinance.Ticker(target_code)
    for idx in result.dividends.index:
        logger.info(idx)
        logger.info(result.dividends[idx])
        upsert_sql = '''INSERT INTO stock.tbl_t_dividend_history(stock_code, dividend_date, dividend_amount, created_at)
        values (%s, %s, %s, now())
        on conflict(stock_code, dividend_date) do update set dividend_amount = %s, updated_at = now()'''
        connection.execute(upsert_sql, (stock_code, idx, result.dividends[idx], result.dividends[idx]))
        logger.info('insert or update stock info done. ')
