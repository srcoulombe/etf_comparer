# standard library dependencies
from typing import Union

# local dependencies
from .dbms.TinyDBDatabaseClient import TinyDBDatabaseClient
from .dbms.SQLite3DatabaseClient import SQLite3DatabaseClient

def select_database(db_type: str) -> Union[TinyDBDatabaseClient, SQLite3DatabaseClient]:
    """Function allowing switches between the database management system
    to use as the backend. 

    Parameters
    ----------
    db_type : str
        String indicating which database management system to use.
        Must be one of ('tinydb','sqlite3').

    Returns
    -------
    Union[TinyDBDatabaseClient, SQLite3DatabaseClient]
        An instantiated client for the chosen database management system to use
        in the backend

    """
    db_type = db_type.lower()
    assert db_type in ('tinydb','sqlite3'), \
        f"db_type must be in ('tinydb','sqlite3'); got {db_type}"
    if db_type == 'tinydb':
        return TinyDBDatabaseClient()
    elif db_type == 'sqlite3':
        return SQLite3DatabaseClient()
    else:
        raise Exception("should not have gone here")

