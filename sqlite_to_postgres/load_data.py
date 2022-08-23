"""Модуль для загрузки данных из SQLite в Postgres."""
import contextlib
import os
import sqlite3

import psycopg2
from postgres_saver import PostgresSaver
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from sqlite_loader import SQLiteLoader

from dotenv import load_dotenv

load_dotenv()


@contextlib.contextmanager
def sqlite3_open_connect(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres.

    Args:
        connection (sqlite3.Connection): коннект к SQLite
        pg_conn (_connection): коннект к Postgres
    """
    postgres_saver = PostgresSaver(pg_conn, schema='content')
    sqlite_loader = SQLiteLoader(connection)

    tables_to_load = {
        'film_work': sqlite_loader.load_movies,
        'person': sqlite_loader.load_persons,
        'genre': sqlite_loader.load_genre,
        'person_film_work': sqlite_loader.load_person_film_work,
        'genre_film_work': sqlite_loader.load_genre_film_work
    }
    for table, sqlite_load in tables_to_load.items():
        postgres_saver.save_data(sqlite_load(), table)


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'),
           'host': os.environ.get('DB_HOST'),
           'port': os.environ.get('DB_PORT')}
    db_path = os.environ.get('DB_SQLITE_PATH')
    with sqlite3_open_connect(db_path) as sqlite_conn:
        with contextlib.closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
