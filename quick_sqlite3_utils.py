# first draft at sqlite3 functionality
# deprecated in favour of `sqlite3_database_client.py`
# kept to show working out

# standard library dependencies
from datetime import datetime
from typing import Union

# external dependencies
import sqlite3
import pandas as pd

def create_connection(path: str):
    connection = None
    
    try:
        connection = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        print("Connection to SQLite DB successful")
    except sqlite3.Error as e:
        print(f"The error '{e}' occurred")
    else:
        connection.execute("PRAGMA foreign_keys = 1")
    return connection

def execute_query(connection, query: str, verbose: bool = False):
    try:
        result = connection.execute(query)
        connection.commit()
        if verbose: 
            print("Query executed successfully")
        return result
    except sqlite3.Error as e:
        print(f"The error '{e}' occurred")

holdings_table_creation_query = '''CREATE TABLE IF NOT EXISTS holdings_table(
    Holding_ID integer PRIMARY KEY,
    Holding varchar(255) unique
)
'''

etf_ticker_table_creation_query = '''CREATE TABLE IF NOT EXISTS etf_ticker_table(
    ETF_ticker_ID integer PRIMARY KEY,
    ETF_ticker varchar(255) unique
)
'''

etf_table_creation_query = '''CREATE TABLE IF NOT EXISTS etf_holdings_table(
    Row_ID integer PRIMARY KEY,
    Date DATE,
    ETF_ticker_ID integer,
    Holding_ID integer,
    Holding_Weight real,
FOREIGN KEY (ETF_ticker_ID) REFERENCES etf_ticker_table (ETF_ticker_ID),
FOREIGN KEY (Holding_ID) REFERENCES holdings_table (Holding_ID)
)
'''

def create_holdings_table(connection) -> None:
    execute_query(connection, holdings_table_creation_query)

def create_etf_ticker_table(connection) -> None:
    execute_query(connection, etf_ticker_table_creation_query)

def create_etf_table(connection) -> None:
    execute_query(connection, etf_table_creation_query)

def setup(db_fp: str = "etf.sqlite"):
    connection = create_connection(db_fp)
    create_holdings_table(connection)
    create_etf_ticker_table(connection)
    create_etf_table(connection)

def get_etf_id_for_ticker(etf_ticker: str) -> Union[None,int]:
    etf_ticker = etf_ticker.upper()
    connection = create_connection("etf.sqlite") 
    etf_id: int = connection.execute(
        "SELECT ETF_ID FROM etf_ticker_table WHERE ETF_ticker = ?", 
        (etf_ticker,)
    ).fetchall()
    return etf_id

def get_holdings_for_etf_id(etf_id: int):
    query = """SELECT major.Date, minor.ETF_ticker, other.Holding, major.Holding_Weight 
    FROM (
        select etf_holdings_table.* from etf_holdings_table where ETF_ticker_ID=? 
        and Date=?
    ) as major 
    INNER JOIN etf_ticker_table as minor on major.ETF_ticker_ID = minor.ETF_ticker_ID 
    LEFT JOIN holdings_table as other on major.Holding_ID=other.Holding_ID"""
    connection = create_connection("etf.sqlite") 
    return connection.execute(
        query, 
        (etf_id, datetime.now().date())
    ).fetchall()

# NOT USED YET
def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    return df

def populate_table_from_csv(connection, 
                            csv_fp: str, 
                            table_name: str = "astronauts", 
                            verbose: bool = False, 
                            **read_csv_kwargs):
    df = pd.read_csv(
        csv_fp,
        **read_csv_kwargs        
    )
    df = clean_df(df)
    if verbose:
        print(f"The dataframe build from {csv_fp} has the following shape: {df.shape}")
        print(df.head())
    
    cursor = connection.execute(f'SELECT * FROM {table_name};')
    field_names = [description[0] for description in cursor.description]

    for field in field_names:
        try: 
            assert field in df.columns
        except AssertionError as missing_field:
            raise AssertionError(f"field {field} is missing") from missing_field

    for index, row in df.iterrows():
        insert_query_values = ",".join([
            f"'{row[field]}'" if isinstance(row[field], str) and row[field] != "NULL" else str(row[field])
            for field in field_names            
        ])
        insert_query = f"INSERT INTO {table_name} ({','.join(field_names)}) VALUES ({insert_query_values});"
        execute_query(connection, insert_query)

if __name__ == '__main__':

    setup()
    connection = create_connection("etf.sqlite") 
    q = """INSERT INTO holdings_table (Holding) VALUES (?)"""
    connection.execute(q, ("ABC",))
    q = """INSERT INTO holdings_table (Holding) VALUES (?)"""
    connection.execute(q, ("DEF",))
    q = """INSERT INTO etf_ticker_table (ETF_ticker) VALUES (?)"""
    connection.execute(q, ("SPY",))
    q = """INSERT INTO etf_ticker_table (ETF_ticker) VALUES (?)"""
    connection.execute(q, ("QQQ",))
    q = '''INSERT INTO etf_holdings_table (Date, ETF_ticker_ID, Holding_ID, Holding_Weight) VALUES (?, ?, ?, ?)'''
    connection.execute(q, (datetime.now().date(), 1,1,3.0))
    connection.execute(q, (datetime.now().date(), 1,2,3.0))
    connection.execute(q, (datetime.now().date(), 2,2,10.0))
    connection.commit()

    connection.execute("SELECT * FROM etf_holdings_table").fetchall()
    connection.execute("SELECT * from etf_ticker_table").fetchall()