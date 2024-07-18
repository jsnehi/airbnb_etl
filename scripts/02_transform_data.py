import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

db_username = os.getenv("USERNAME")
db_password = os.getenv("PASSWORD")
db_host = os.getenv("HOST")
db_port = os.getenv("PORT")
db_name = os.getenv("DB_NAME")

engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

df = pd.read_sql_query('SELECT * FROM airbnb_data', engine)

avg_price_per_neighborhood = df.groupby('neighbourhood')['price'].mean().reset_index()

df.fillna(method='ffill', inplace=True)

avg_price_per_neighborhood.to_sql('avg_price_per_neighborhood', engine, if_exists='replace', index=False)
