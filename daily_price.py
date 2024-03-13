from dotenv import load_dotenv
import requests
import db_connection
import my_logger
import json
from datetime import date, timedelta

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()


def all_stock_all_days():
    id_token_sql = '''select token from feed.tbl_t_token where env_type = 3'''
    id_token = connection.select_one(id_token_sql)

    select_sql = '''select stock_code from stock.tbl_m_stock_info'''
    stock_codes = connection.select_all(select_sql)
    # logger.info('stock_codes: %s', stock_codes)

    for stock_code in stock_codes:
        target_code = stock_code[0]
        logger.info('stock_code: %s', target_code)

        api_url = 'https://api.jquants.com/v1/prices/daily_quotes'
        headers = {
            'Authorization': 'Bearer ' + id_token
        }

        response = requests.get(api_url, headers=headers, params={'code': target_code})
        json_data = response.json()
        daily_quotes = json_data['daily_quotes']
        # json log
        # logger.info(json.dumps(daily_quotes[0], indent=2, ensure_ascii=False))
        for daily_quote in daily_quotes:
            insert_sql = '''INSERT INTO stock.tbl_t_daily_price(stock_code, date, open, high, low, close,
             upper_limit,lower_limit, volume, turnover_value, adjustment_factor, adjustment_open,
             adjustment_high,adjustment_low, adjustment_close, adjustment_volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            try:
                connection.no_commit_execute(insert_sql,
                                             (target_code,
                                              daily_quote['Date'],
                                              daily_quote['Open'],
                                              daily_quote['High'],
                                              daily_quote['Low'],
                                              daily_quote['Close'],
                                              daily_quote['UpperLimit'],
                                              daily_quote['LowerLimit'],
                                              daily_quote['AdjustmentVolume'],
                                              daily_quote['TurnoverValue'],
                                              daily_quote['AdjustmentFactor'],
                                              daily_quote['AdjustmentOpen'],
                                              daily_quote['AdjustmentHigh'],
                                              daily_quote['AdjustmentLow'],
                                              daily_quote['AdjustmentClose'],
                                              daily_quote['AdjustmentVolume']
                                              ))
            except Exception as e:
                logger.error(e)
                continue
        connection.commit()
        logger.info('fetch stock info done')


def all_stock_today(pagination_key=None):
    id_token_sql = '''select token from feed.tbl_t_token where env_type = 3'''
    id_token = connection.select_one(id_token_sql)

    # logger.info('stock_code: %s', target_code)

    api_url = 'https://api.jquants.com/v1/prices/daily_quotes'
    headers = {
        'Authorization': 'Bearer ' + id_token
    }
    today = date.today() - timedelta(days=1)
    if pagination_key is not None:
        response = requests.get(api_url, headers=headers,
                                params={'date': today, 'pagination_key': pagination_key})
    else:
        response = requests.get(api_url, headers=headers, params={'date': today})

    if 'pagination_key' in response.json():
        logger.info(response.json()['pagination_key'])
    json_data = response.json()

    daily_quotes = json_data['daily_quotes']
    # json log
    logger.info(json.dumps(daily_quotes[0], indent=2, ensure_ascii=False))
    for daily_quote in daily_quotes:
        upsert_sql = '''INSERT INTO stock.tbl_t_daily_price(stock_code, date, open, high, low, close,
             upper_limit,lower_limit, volume, turnover_value, adjustment_factor, adjustment_open,
             adjustment_high,adjustment_low, adjustment_close, adjustment_volume) 
             VALUES (%(Code)s, %(Date)s, %(Open)s, %(High)s, %(Low)s, %(Close)s, %(UpperLimit)s, %(LowerLimit)s,
              %(AdjustmentVolume)s, %(TurnoverValue)s, %(AdjustmentFactor)s, %(AdjustmentOpen)s, %(AdjustmentHigh)s,
               %(AdjustmentLow)s, %(AdjustmentClose)s, %(AdjustmentVolume)s)
             on conflict(stock_code, date) do update set open = %(Open)s, high = %(High)s, low = %(Low)s,
              close = %(Close)s, upper_limit = %(UpperLimit)s, lower_limit = %(LowerLimit)s, 
              volume = %(AdjustmentVolume)s, turnover_value = %(TurnoverValue)s, 
              adjustment_factor = %(AdjustmentFactor)s, adjustment_open = %(AdjustmentOpen)s,
               adjustment_high = %(AdjustmentHigh)s, adjustment_low = %(AdjustmentLow)s, 
               adjustment_close = %(AdjustmentClose)s, adjustment_volume = %(AdjustmentVolume)s'''

        try:
            connection.execute(upsert_sql, daily_quote)
        except Exception as e:
            logger.error(e)
            break
    if 'pagination_key' in response.json():
        logger.info('continue request next page %s' % response.json()['pagination_key'])
        all_stock_today(response.json()['pagination_key'])
    logger.info('fetch stock info done')


all_stock_today()
