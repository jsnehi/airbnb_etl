import os
from metaflow import FlowSpec, step #, Parameter
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class ETLFlow(FlowSpec):
    def __init__(self):
        # Define PostgreSQL connection parameters
        self.db_username = os.getenv("USERNAME")
        self.db_password = os.getenv("PASSWORD")
        self.db_host = os.getenv("HOST")
        self.db_port = os.getenv("PORT")
        self.db_name = os.getenv("DB_NAME")

    @step
    def start(self):
        self.next(self.load_data)

    @step
    def load_data(self):
        engine = create_engine(f'postgresql://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        self.df = pd.read_csv('data/nyc_airbnb_data.csv')
        self.df.to_sql('airbnb_data', engine, if_exists='replace', index=False)

        self.next(self.transform_data)

    @step
    def transform_data(self):
        engine = create_engine(f'postgresql://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}')
        df = pd.read_sql_query('SELECT * FROM airbnb_data', engine)

        # Data transformation steps
        df['date'] = pd.to_datetime(df['date'])
        df['time'] = df['date'].dt.time

        #Calculate average price per neighborhood
        avg_price_per_neighborhood = df.groupby('neighborhood')['price'].mean().reset_index()

        avg_price_per_neighborhood.to_sql('avg_price_per_neighborhood', engine, if_exists='replace', index=False)

        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    ETLFlow()
