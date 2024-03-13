import json
import os
from dotenv import load_dotenv
from logging import getLogger, basicConfig, INFO
import requests
import db_connection

load_dotenv()
logger = getLogger(__name__)
logging_format = "%(asctime)s\t%(levelname)s\t%(name)s\t%(filename)s\t%(funcName)s\t%(lineno)d\t%(message)s"
basicConfig(level=INFO, format=logging_format)

connection = db_connection.DatabaseConnection()

token_sql = '''select token  from kabu.token where env_type = 0'''
api_url = os.getenv('API_URL')

token = connection.select_one(token_sql)
logger.info(token)
position_url = api_url + '/positions'
logger.info('request_url: %s', position_url)
headers = {
    'x-api-key': token
}
logger.info(headers)
# response = requests.get(position_url, headers=headers)
# data = response.content.decode('utf-8')
# logger.info(data)
# json_data = json.loads(data)[0]

# logger.info(json.dumps(json_data, indent=2, ensure_ascii=False))
#
# logger.info(json_data['SymbolName'])
# currentPrice = json_data['CurrentPrice']

cash_url = api_url + '/wallet/margin'
logger.info('request_url: %s', cash_url)
response = requests.get(cash_url, headers=headers)
data = response.content.decode('utf-8')
logger.info(data)
json_data = json.loads(data)

logger.info(json.dumps(json_data, indent=2, ensure_ascii=False))
#
# if currentPrice < 21:
#     logger.info('currentPrice: %s', currentPrice)
#     logger.info('currentPrice is less than 21')
#
#
# ranking_url = api_url + '/ranking'
# logger.info('request_url: %s', ranking_url)
# response = requests.get(ranking_url, headers=headers, params={'Type': '5'})
# data = response.content.decode('utf-8')
# json_data = json.loads(data)
# logger.info(json.dumps(json_data, indent=2, ensure_ascii=False))


info_url = api_url + '/symbol/9434@1'
logger.info('request_url: %s', info_url)
response = requests.get(info_url, headers=headers, params={'addinfo': 'true'})
data = response.content.decode('utf-8')
json_data = json.loads(data)
logger.info(json.dumps(json_data, indent=2, ensure_ascii=False))

board_url = api_url + '/board/9434@1'
logger.info('request_url: %s', board_url)
response = requests.get(board_url, headers=headers)
data = response.content.decode('utf-8')
json_data = json.loads(data)
logger.info(json.dumps(json_data, indent=2, ensure_ascii=False))

register_url = api_url + '/register'
logger.info('request_url: %s', register_url)
symbols = {"Symbols": [{"Symbol": "3778", "Exchange": 1}]}
response = requests.put(register_url, json=symbols, headers=headers)
data = response.content.decode('utf-8')
json_data = json.loads(data)
logger.info(json.dumps(json_data, indent=2, ensure_ascii=False))

send_order_url = api_url + '/sendorder'
password = os.getenv('PASSWORD')
order = {
    "Password": password,
    "Symbol": "6740",
    "Exchange": 1,
    "SecurityType": 1,
    "Side": "1",
    "CashMargin": 1,
    "DelivType": 2,
    "AccountType": 4,
    "FundType": "  ",
    "Qty": 100,
    "FrontOrderType": 30,
    "Price": 0,
    "ExpireDay": 0,
    "ReverseLimitOrder": {
        "TriggerSec": 1,
        "TriggerPrice": 15,
        "UnderOver": 1,
        "AfterHitOrderType": 2,
        "AfterHitPrice": 30
    }
}
# response = requests.post(send_order_url, json=order, headers=headers)
# data = response.content.decode('utf-8')
# logger.info(data)
