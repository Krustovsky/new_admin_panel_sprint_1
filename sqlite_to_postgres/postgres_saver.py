"""Основной класс для сохранения данных в Postgres."""

from dataclasses import astuple
from movies_data import FilmWork, Person, GenreFilmWork, Genre, PersonFilmWork

from psycopg2.extras import execute_batch


class PostgresSaver:
    def __init__(self, pg_conn, page_size=500, schema=''):
        """Создаем объект с коннектом и курсором.

        Args:
            pg_conn (psycopg2.extras.connection): коннект к Postgres
            page_size (int): для execute_batch
            schema (str): имя схемы в которую пишем
        """
        self.conn = pg_conn
        self.cur = pg_conn.cursor()
        self.page_size = page_size
        if schema:
            self.schema: str = '{0}.'.format(schema)
        else:
            self.schema = schema
        self.tables = {'film_work': FilmWork,
                       'person': Person,
                       'genre': Genre,
                       'person_film_work': PersonFilmWork,
                       'genre_film_work': GenreFilmWork
                       }

    ['film_work', 'genre_film_work', 'person_film_work', 'genre', 'person']

    def save_data(self, loaded_data: list, table: str):
        """Сохранение данных в таблицу Postgres.

        Args:
            loaded_data: данные для загрузки (генератор)
            table (str): имя таблицы в которую грузим данные
        """
        if table not in ['film_work', 'genre_film_work', 'person_film_work', 'genre', 'person']:
            raise Exception('Не указана теблица или такой таблицы нет в списке'
                            'film_work, genre_film_work, person_film_work, genre, person')

        data_class = self.tables[table]
        data_to_save = []
        for row_batch in loaded_data:
            data_to_save += [astuple(data_class(**dict(_))) for _ in row_batch]
        query = self._table_query(self.schema, table, data_class)
        execute_batch(self.cur, query, data_to_save, page_size=self.page_size)
        self.conn.commit()

    def _table_query(self, schema, table, data_class):
        """Метод для созданий query из полей датакласса.

        Args:
            schema: схема в БД Postgres куда грузим данные
            table: таблица в БД Postgres куда грузим данные
            data_class: тип данных которые грузим для создания полей

        Returns:
            str: insert query для запроса в БД

        """
        insert_into = f"INSERT INTO {schema}{table} ({', '.join(data_class.__dataclass_fields__.keys())})"
        values_to_insert = f"VALUES ({', '.join(['%s' for _ in data_class.__dataclass_fields__.keys()])})"
        on_conflict = 'ON CONFLICT (id) DO NOTHING'

        return '{0} {1} {2}'.format(insert_into, values_to_insert, on_conflict)
