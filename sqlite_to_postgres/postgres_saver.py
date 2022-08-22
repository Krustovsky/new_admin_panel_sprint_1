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

    def save_film_work(self, data_to_save: list):
        """Сохранение данных в таблицу Postgres film_work.

        Args:
            data_to_save: массив данных для загрузки
        """
        table = 'film_work'
        data_class = FilmWork
        data_to_save = [astuple(data_class(**dict(_))) for _ in data_to_save]
        query = self.__table_query(self.schema, table, data_class)
        execute_batch(self.cur, query, data_to_save, page_size=self.page_size)
        self.conn.commit()

    def save_persons(self, data_to_save: list):
        """Сохранение данных в таблицу Postgres person.

        Args:
            data_to_save: массив данных для загрузки
        """
        table = 'person'
        data_class = Person
        data_to_save = [astuple(data_class(**dict(_))) for _ in data_to_save]
        query = self.__table_query(self.schema, table, data_class)
        execute_batch(self.cur, query, data_to_save, page_size=self.page_size)
        self.conn.commit()

    def save_genre(self, data_to_save: list):
        """Сохранение данных в таблицу Postgres genre.

        Args:
            data_to_save: массив данных для загрузки
        """
        table = 'genre'
        data_class = Genre
        data_to_save = [astuple(data_class(**dict(_))) for _ in data_to_save]
        query = self.__table_query(self.schema, table, data_class)
        execute_batch(self.cur, query, data_to_save, page_size=self.page_size)
        self.conn.commit()

    def save_person_film_work(self, data_to_save: list):
        """Сохранение данных в таблицу Postgres person_film_work.

        Args:
            data_to_save: массив данных для загрузки
        """
        table = 'person_film_work'
        data_class = PersonFilmWork
        data_to_save = [astuple(data_class(**dict(_))) for _ in data_to_save]
        query = self.__table_query(self.schema, table, data_class)
        execute_batch(self.cur, query, data_to_save, page_size=self.page_size)
        self.conn.commit()

    def save_genre_film_work(self, data_to_save: list):
        """Сохранение данных в таблицу Postgres genre_film_workk.

        Args:
            data_to_save: массив данных для загрузки
        """
        table = 'genre_film_work'
        data_class = GenreFilmWork
        data_to_save = [astuple(data_class(**dict(_))) for _ in data_to_save]
        query = self.__table_query(self.schema, table, data_class)
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
