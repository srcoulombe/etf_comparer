# standard library dependencies
import logging
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
    log = logging.getLogger("root")

    db_type = db_type.lower()
    if not db_type in ('tinydb','sqlite3'):
        log.error(f"`db_type` argument for `select_database` must be in ('tinydb','sqlite3'); got {db_type}. Reverting back to {db_type}.")
    if db_type == 'tinydb':
        log.info("Connecting to TinyDB client instance")
        return TinyDBDatabaseClient()
    elif db_type == 'sqlite3':
        log.info(f"Connecting to SQLite3 client instance")
        return SQLite3DatabaseClient()
    else:
        raise Exception("should not have gone here")

