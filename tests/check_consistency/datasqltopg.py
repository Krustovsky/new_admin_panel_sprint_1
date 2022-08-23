"""Основной модель для проверки соответствия данных между SQLite и Postgres."""
import contextlib
import sqlite3
import os

import psycopg2
from postgres_check import PostgresChecker
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from sqlite_check import SQLiteChecker

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


def data_compare_sqlite_to_pg(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод сравнения данных имежду SQLite и Postgres.

    Args:
        connection (sqlite3.Connection): коннект к SQLite
        pg_conn (_connection): коннект к Postgres
    """
    postgres_saver = PostgresChecker(pg_conn, schema='content')
    sqlite_loader = SQLiteChecker(connection)
    tables = ['film_work', 'genre_film_work', 'person_film_work', 'genre', 'person']
    # проверяем целостности таблиц
    for table in tables:
        sqlite_table_rows = sqlite_loader.load_table_row_count(table)
        pg_table_rows = postgres_saver.load_table_count(table)
        assert sqlite_table_rows == pg_table_rows, \
            '\nВ таблицах {0} разные данные {1} vs {2}'.format(table, sqlite_table_rows, pg_table_rows)

    # попарное сравнение данных
    for table in tables:
        offset = 0
        while True:
            target_data = postgres_saver.load_table_to_campare(table, offset=offset)
            sourse_data = sqlite_loader.load_table_to_campare(table, offset=offset)

            if target_data:
                for sqlite_row, pg_row in zip(sourse_data, target_data):
                    assert sqlite_row == pg_row, f'\n{sqlite_row}\nis not\n{pg_row}'
                offset += 100
            else:
                break


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'),
           'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'),
           'host': os.environ.get('DB_HOST'),
           'port': os.environ.get('DB_PORT')}
    db_path = os.environ.get('DB_SQLITE_PATH')
    with sqlite3_open_connect(db_path) as sqlite_conn:
        with contextlib.closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
            data_compare_sqlite_to_pg(sqlite_conn, pg_conn)
