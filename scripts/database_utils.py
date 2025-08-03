import mysql.connector
import clickhouse_connect
import pandas as pd
import logging
from typing import Optional, List, Dict, Any
from config import DatabaseConfig

class MySQLConnector:
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database
            )
            logging.info("MySQL connection established")
        except Exception as e:
            logging.error(f"Failed to connect to MySQL: {e}")
            raise
            
    def execute_query(self, query: str) -> pd.DataFrame:
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            return pd.read_sql(query, self.connection)
        except Exception as e:
            logging.error(f"Failed to execute MySQL query: {e}")
            raise
            
    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL connection closed")

class ClickHouseConnector:
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.client = None
        
    def connect(self):
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.user,
                password=self.config.password,
                database=self.config.database
            )
            logging.info("ClickHouse connection established")
        except Exception as e:
            logging.error(f"Failed to connect to ClickHouse: {e}")
            raise
            
    def execute_query(self, query: str) -> pd.DataFrame:
        try:
            if not self.client:
                self.connect()
            result = self.client.query(query)
            return result.result_rows
        except Exception as e:
            logging.error(f"Failed to execute ClickHouse query: {e}")
            raise
            
    def insert_dataframe(self, table_name: str, df: pd.DataFrame):
        try:
            if not self.client:
                self.connect()
            self.client.insert_df(table_name, df)
            logging.info(f"Inserted {len(df)} rows into {table_name}")
        except Exception as e:
            logging.error(f"Failed to insert data into {table_name}: {e}")
            raise
            
    def execute_command(self, command: str):
        try:
            if not self.client:
                self.connect()
            self.client.command(command)
        except Exception as e:
            logging.error(f"Failed to execute ClickHouse command: {e}")
            raise