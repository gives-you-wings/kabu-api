from dotenv import load_dotenv
import requests
import db_connection
import my_logger
import json

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()

id_token_sql = '''select token from feed.tbl_t_token where env_type = 3'''
id_token = connection.select_one(id_token_sql)

info_url = 'https://api.jquants.com/v1/listed/info'
headers = {
    'Authorization': 'Bearer ' + id_token
}

response = requests.get(info_url, headers=headers)
json_data = response.json()
info = json_data['info']
# info count
logger.info(len(info))
# json log
# logger.info(json.dumps(info, indent=2, ensure_ascii=False))

upsert_sql = '''INSERT INTO stock.tbl_m_stock_info(stock_code,
 company_name, 
 sector_17_code, 
 sector_17_code_name, 
 sector_33_code,
 sector_33_code_name,
 scale_category,
 market_code,
 market_code_name,
 margin_code,
 margin_code_name,
 created_at) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
on conflict(stock_code) do update set company_name = %s, sector_17_code = %s, sector_17_code_name = %s, sector_33_code = %s, sector_33_code_name = %s, scale_category = %s, market_code = %s, market_code_name = %s, margin_code = %s, margin_code_name = %s, updated_at = now()'''

for stock_info in info:
    # logger.info(json.dumps(stock_info, indent=2, ensure_ascii=False))
    connection.no_commit_execute(upsert_sql, (
        stock_info['Code'],
        stock_info['CompanyName'],
        stock_info['Sector17Code'],
        stock_info['Sector17CodeName'],
        stock_info['Sector33Code'],
        stock_info['Sector33CodeName'],
        stock_info['ScaleCategory'],
        stock_info['MarketCode'],
        stock_info['MarketCodeName'],
        None,
        None,
        stock_info['CompanyName'],
        stock_info['Sector17Code'],
        stock_info['Sector17CodeName'],
        stock_info['Sector33Code'],
        stock_info['Sector33CodeName'],
        stock_info['ScaleCategory'],
        stock_info['MarketCode'],
        stock_info['MarketCodeName'],
        None,
        None))
connection.commit()
logger.info('insert or update stock info done. ')
