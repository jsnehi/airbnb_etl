import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
df = pd.read_csv('data/AB_NYC_2019.csv')

db_username = os.getenv("USERNAME")
db_password = os.getenv("PASSWORD")
db_host = os.getenv("HOST")
db_port = os.getenv("PORT")
db_name = os.getenv("DB_NAME")

engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

df.to_sql('airbnb_data', engine, if_exists='replace', index=False)
