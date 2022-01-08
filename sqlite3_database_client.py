# standard library dependencies
import sqlite3
from datetime import datetime
from typing import List, Mapping, Tuple, Union, Iterable, Any

# local dependencies
from scraping import scrape_etf_holdings
# import filepath from .yaml file  

class SQLite3DatabaseClient:
    def __init__(self, path_to_db: str = "etf.sqlite") -> None:
        self.__path_to_db = path_to_db
    
    def connect(self) -> sqlite3.Connection:
        connection = None
        try:
            connection = sqlite3.connect(
                self.__path_to_db,
                detect_types=sqlite3.PARSE_DECLTYPES
            )
        except sqlite3.Error as e:
            # log
            print(f"The sqlite3.Error '{e}' occurred")
            raise e
        except Exception as e:
            print(f"The error '{e}' occurred")
            raise e
        else:
            connection.execute("PRAGMA foreign_keys = 1")
            return connection

    def execute_query(self, query: str, *args) -> list:
        connection = self.connect()
        try:
            cursor = connection.execute(query, *args)
            connection.commit()
            result = cursor.fetchall()
            cursor.close()
        except sqlite3.Error as e:
            # log
            print(f"The sqlite3.Error '{e}' occurred")
            connection.rollback()
            connection.close()
            raise e
        except Exception as e:
            print(f"The error '{e}' occurred")
            connection.rollback()
            connection.close()
            raise e
        else:
            connection.close()
            return result

    def execute_query_over_many_arguments(self, query: str, args: Iterable[Any]):
        connection = self.connect()
        try:
            cursor = connection.executemany(
                query, 
                args
            )
            connection.commit()
            result = cursor.fetchall()
            cursor.close()
        except sqlite3.Error as e:
            # log
            print(f"The sqlite3.Error '{e}' occurred")
            connection.rollback()
            connection.close()
            raise e
        except Exception as e:
            print(f"The error '{e}' occurred")
            connection.rollback()
            connection.close()
            raise e
        else:
            connection.close()
            return result

    @property
    def holdings_table_creation_query(self) -> str:
        return '''CREATE TABLE IF NOT EXISTS holdings_table(
            Holding_ID integer PRIMARY KEY,
            Holding varchar(255) unique
        )
        '''

    @property
    def etf_ticker_table_creation_query(self) -> str:
        return '''CREATE TABLE IF NOT EXISTS etf_ticker_table(
            ETF_ticker_ID integer PRIMARY KEY,
            ETF_ticker varchar(255) unique
        )
        '''

    @property
    def etf_table_creation_query(self) -> str:
        return '''CREATE TABLE IF NOT EXISTS etf_holdings_table(
            Row_ID integer PRIMARY KEY,
            Date DATE,
            ETF_ticker_ID integer,
            Holding_ID integer,
            Holding_Weight real,
        FOREIGN KEY (ETF_ticker_ID) REFERENCES etf_ticker_table (ETF_ticker_ID),
        FOREIGN KEY (Holding_ID) REFERENCES holdings_table (Holding_ID)
        )
        '''

    def create_holdings_table(self) -> None:
        self.execute_query(self.holdings_table_creation_query)

    def create_etf_ticker_table(self) -> None:
        self.execute_query(self.etf_ticker_table_creation_query)

    def create_etf_table(self) -> None:
        self.execute_query(self.etf_table_creation_query)

    def setup(self) -> None:
        self.create_holdings_table()
        self.create_etf_ticker_table()
        self.create_etf_table()

    def get_known_etfs(self) -> List[str]:
        res = self.execute_query("SELECT ETF_ticker from etf_ticker_table")
        return [ ticker for (ticker, ) in res ]

    def get_etf_id_for_ticker(  self, 
                                etf_ticker: str) -> Union[None,int]:
        etf_ticker = etf_ticker.upper()
        etf_ids: List[Tuple[int]] = self.execute_query(
            "SELECT ETF_ticker_ID FROM etf_ticker_table WHERE ETF_ticker = ?", 
            (etf_ticker,)
        )
        try:     
            etf_id: int = etf_ids[0][0]
        except IndexError as no_data_for_etf:
            message = f"No data for ETF: '{etf_ticker}'"
            raise ValueError(message) from no_data_for_etf
        else:
            return etf_id

    def get_holdings_and_weights_for_etf(   self, 
                                            etf_ticker: str) -> List[Tuple[datetime.date, str, str, float]]:
        # a good test: datetime = today
        query = """SELECT major.Date, minor.ETF_ticker, other.Holding, major.Holding_Weight 
        FROM (
            select etf_holdings_table.* from etf_holdings_table where ETF_ticker_ID=? 
            and Date=?
        ) as major 
        INNER JOIN etf_ticker_table as minor on major.ETF_ticker_ID = minor.ETF_ticker_ID 
        LEFT JOIN holdings_table as other on major.Holding_ID=other.Holding_ID
        """
        try:
            etf_id = self.get_etf_id_for_ticker(etf_ticker)
            holdings: List[Tuple[datetime.date, str, str, float]] = self.execute_query(
                query, 
                (etf_id, datetime.now().date())
            )
        except ValueError as no_current_data_for_etf:
            etf_holdings = scrape_etf_holdings(etf_ticker)
            self.execute_query(
                "insert into etf_ticker_table (ETF_ticker) values (?)",
                (etf_ticker,)
            )
            etf_ticker_id = self.get_etf_id_for_ticker(etf_ticker)
            holding_tickers = [(ticker,) for ticker, weight in etf_holdings]
            self.execute_query_over_many_arguments(
                "insert or ignore into holdings_table (Holding) values (?)",
                holding_tickers
            )
            # OK we can't use executemany for SELECT queries
            # holding_tickers_and_id = self.execute_query_over_many_arguments(
            #     "select * from holdings_table where Holding = (?)",
            #     holding_tickers
            # )
            holding_tickers_and_id: List[Tuple[str, int]] = []
            for (holding_ticker, ) in holding_tickers:
                holding_id = self.execute_query(
                    "select Holding_ID from holdings_table where Holding = ?",
                    (holding_ticker,)
                )[0][0]
                holding_tickers_and_id.append((holding_ticker, holding_id))
            today = datetime.now().date()
            params = [
                (today, etf_ticker_id, holding_id, holding_weight)
                for (holding_ticker, holding_id), holding_weight in 
                zip(holding_tickers_and_id, [weight for ticker, weight in etf_holdings]) 
            ]
            self.execute_query_over_many_arguments(
                f"""insert into etf_holdings_table 
                (Date, ETF_ticker_ID, Holding_ID, Holding_Weight)
                values (?, ?, ?, ?)
                """,
                params
            )
            holdings: List[Tuple[datetime.date, str, str, float]] = self.execute_query(
                query, 
                (etf_ticker_id, datetime.now().date())
            )
        
        return holdings
    
    def get_holdings_and_weights_for_etfs(  self, 
                                            etf_tickers: List[str]) -> Mapping[str, Mapping[str, Mapping]]:
        results: Mapping[str, Mapping[str, Mapping]] = dict()
        for etf_ticker in etf_tickers:
            etf_ticker_holdings: List[Tuple[datetime.date, str, str, float]] = self.get_holdings_and_weights_for_etf(etf_ticker)
            results[etf_ticker] = {
                holding_ticker: dict(weight=holding_weight)
                for (date, etf_ticker_id, holding_ticker, holding_weight)
                in etf_ticker_holdings
            }
        return results