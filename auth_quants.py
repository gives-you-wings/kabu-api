from dotenv import load_dotenv
import db_connection
import my_logger
import requests

load_dotenv()
logger = my_logger.MyLogger(__name__).logger
connection = db_connection.DatabaseConnection()


def fetch_id_token():
    select_refresh_token_sql = '''select token from feed.tbl_t_token where env_type = 2'''
    refresh_token = connection.select_one(select_refresh_token_sql)
    logger.info('refresh_token: %s', refresh_token)

    id_token_url = 'https://api.jquants.com/v1/token/auth_refresh'
    response = requests.post(id_token_url, params={'refreshtoken': refresh_token})
    logger.info('response: %s', response.content.decode('utf-8'))
    data = response.content.decode('utf-8')
    json_data = response.json()
    id_token = json_data['idToken']
    logger.info('id_token: %s', id_token)

    upsert_sql = '''INSERT INTO feed.tbl_t_token(env_type, token, created_at) VALUES (%s, %s, now())
    on conflict(env_type) do update set token = %s, created_at = now()'''
    connection.execute(upsert_sql, (3, id_token, id_token))


fetch_id_token()
