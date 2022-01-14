# standard library dependencies
from typing import Iterable, Mapping, Union

# local dependencies
from tinydb_database_client import TinyDBDatabaseClient
from sqlite3_database_client import SQLite3DatabaseClient

def select_database(db_type: str) -> Union[TinyDBDatabaseClient, SQLite3DatabaseClient]:
    db_type = db_type.lower()
    assert db_type in ('tinydb','sqlite3'), \
        f"db_type must be in ('tinydb','sqlite3'); got {db_type}"
    if db_type == 'tinydb':
        return TinyDBDatabaseClient()
    elif db_type == 'sqlite3':
        return SQLite3DatabaseClient()
    else:
        raise Exception("should not have gone here")

