import time
from airflow_client.client import ApiClient
from pprint import pprint
from airflow_client.client import Configuration
from airflow_client.client.api import dag_api
from dotenv import load_dotenv
import os
import urllib3

load_dotenv(override=True)

url = os.getenv("AIRFLOW_DEV_URL")
username = os.getenv("AIRFLOW_USERNAME")
password = os.getenv("AIRFLOW_PASSWORD")


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

configuration = Configuration(
    host=url, username=username, password=password
)

configuration.verify_ssl = False

client = ApiClient(configuration)



