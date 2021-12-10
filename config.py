import os

from dotenv import load_dotenv

load_dotenv()

application_id = os.getenv('APPLICATION_ID')
db_name = os.getenv('DB_NAME')
db_username = os.getenv('DB_USERNAME')
db_pass = os.getenv('DB_PASS')

core_url = "https://api.worldoftanks.ru/wot/"
