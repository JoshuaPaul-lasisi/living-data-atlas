import os # to read environment variables
import requests #for making HTTP requests
import pandas as pd # for data manipulation
from sqlalchemy import create_engine # to connect to Postgres database
from dotenv import load_dotenv # to load what we read from the env file
from datetime import datetime # so we can work with dates

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')