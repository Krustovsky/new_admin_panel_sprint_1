"""Модуль для загрузки данных из SQLite в Postgres."""
import contextlib
import sqlite3

import psycopg2
from postgres_saver import PostgresSaver
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from sqlite_loader import SQLiteLoader


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres.

    Args:
        connection (sqlite3.Connection): коннект к SQLite
        pg_conn (_connection): коннект к Postgres
    """
    postgres_saver = PostgresSaver(pg_conn, schema='content')
    sqlite_loader = SQLiteLoader(connection)

    table_data = sqlite_loader.load_movies()
    postgres_saver.save_film_work(table_data)

    table_data = sqlite_loader.load_genre()
    postgres_saver.save_genre(table_data)

    table_data = sqlite_loader.load_persons()
    postgres_saver.save_persons(table_data)

    table_data = sqlite_loader.load_person_film_work()
    postgres_saver.save_person_film_work(table_data)

    table_data = sqlite_loader.load_genre_film_work()
    postgres_saver.save_genre_film_work(table_data)


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '192.168.1.23', 'port': 5432}
    db_path = 'db.sqlite'
    with sqlite3.connect(db_path) as sqlite_conn:
        with contextlib.closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
