import os
from dotenv import load_dotenv
import requests
import db_connection
import my_logger

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()

token_url = os.getenv('API_URL') + '/token'
headers = {
    'Content-Type': 'application/json',
}
response = requests.post(token_url, json={'APIPassword': os.getenv('API_PASSWORD')}, headers=headers)
# res = json.loads(response.json())
token = response.json()['Token']
os.environ['TOKEN'] = token
logger.info(token)

upsert_sql = '''INSERT INTO feed.tbl_t_token(env_type, token, created_at) VALUES (%s, %s, now()) 
on conflict(env_type) do update set token = %s, created_at = now()'''
connection.execute(upsert_sql, (0, token, token))
