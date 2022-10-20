from metabasepy import Client, RequestException
import os
from os.path import exists
from dotenv import load_dotenv

dotenv_path = '.env'
if not os.path.isfile(dotenv_path):
    raise RuntimeError('Missing .env file with credentials; '
                       'ask maintainer for one '
                       'or create and populate .env file manually')
load_dotenv(dotenv_path)

BASE_URL = os.getenv("METABASE_BASE_URI")
ADMIN_EMAIL = os.getenv("NOCO_ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("NOCO_ADMIN_PASSWORD")
GOOGLE_CREDENTIALS = os.getenv("TARGET_BIGQUERY_CREDENTIALS_PATH")
SMTP_HOST=os.getenv("SMTP_HOST")
SMTP_PORT=os.getenv("SMTP_PORT")
SMTP_SECURITY=os.getenv("SMTP_SECURITY")
SMTP_USERNAME=os.getenv("SMTP_USERNAME")
SMTP_PASSWORD=os.getenv("SMTP_PASSWORD")
SMTP_FROM_NAME=os.getenv("SMTP_FROM_NAME")
SMTP_FROM_ADDRESS=os.getenv("SMTP_FROM_ADDRESS")


client = Client(username=ADMIN_EMAIL, password=ADMIN_PASSWORD, base_url=BASE_URL)

client.setup(path_to_cred_file=GOOGLE_CREDENTIALS)
client.authenticate()
client.setup_email(
    SMTP_HOST,
    SMTP_PORT,
    SMTP_SECURITY,
    SMTP_USERNAME,
    SMTP_PASSWORD,
    SMTP_FROM_NAME,
    SMTP_FROM_ADDRESS
)
