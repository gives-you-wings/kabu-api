import json
import os
from dotenv import load_dotenv
import requests
import db_connection
import my_logger

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()

token_sql = '''select token  from kabu.token where env_type = 0'''
api_url = os.getenv('API_URL')
token = connection.select_one(token_sql)
logger.info(token)

ranking_url = api_url + '/ranking'
logger.info('request_url: %s', ranking_url)
headers = {
    'x-api-key': token
}
response = requests.get(ranking_url, headers=headers, params={'Type': '1'})
data = response.content.decode('utf-8')
json_data = json.loads(data)
no_1 = json_data['Ranking'][0]
logger.info(json.dumps(no_1, indent=2, ensure_ascii=False))

register_url = api_url + '/register'
logger.info('request_url: %s', register_url)
no_1_symbol = no_1['Symbol']
symbols = {"Symbols": [{"Symbol": no_1_symbol, "Exchange": 1}]}
response = requests.put(register_url, headers=headers, json=symbols)
data = response.content.decode('utf-8')
json_data = json.loads(data)
logger.info(json.dumps(json_data, indent=2, ensure_ascii=False))
