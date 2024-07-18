# etl_flow.py

from metaflow import FlowSpec, step #, Parameter
from sqlalchemy import create_engine
import pandas as pd
# configure(plugins={'kubernetes': False})

class ETLFlow(FlowSpec):
    def __init__(self):
        # Define PostgreSQL connection parameters
        self.db_username = 'postgres'
        self.db_password = 'admin'
        self.db_host = 'localhost'
        self.db_port = '5433'
        self.db_name = 'airbnb_db'

    @step
    def start(self):
        # self.db_username = Parameter('db_username', help='Username for PostgreSQL')
        # self.db_password = Parameter('db_password', help='Password for PostgreSQL')
        # self.db_host = Parameter('db_host', help='Host for PostgreSQL')
        # self.db_port = Parameter('db_port', help='Port for PostgreSQL')
        # self.db_name = Parameter('db_name', help='Database name')

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

        # Example: Calculate average price per neighborhood
        avg_price_per_neighborhood = df.groupby('neighborhood')['price'].mean().reset_index()

        avg_price_per_neighborhood.to_sql('avg_price_per_neighborhood', engine, if_exists='replace', index=False)

        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == '__main__':
    ETLFlow()
