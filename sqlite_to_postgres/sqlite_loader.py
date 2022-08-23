"""Модуль для загрузки данных из таблиц SQLite."""
import sqlite3


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection, fetch_size: int = 100):
        """Объект для коннекта к SQLite.

        Args:
            connection (sqlite3.Connection): коннект к SQLite
            fetch_size (int): размер одного запроса в БД
        """
        self.connection: sqlite3.Connection = connection
        self.connection.row_factory = sqlite3.Row
        self.curs = self.connection.cursor()
        self.fetch_size: int = fetch_size

    def load_movies(self):
        """Загрузка данных из таблицы film_work.

        Returns:
            results: Массив sqlite3.Row (генератор)
        """
        self.curs.execute('SELECT id, title, description, '
                          'creation_date, rating, type, created_at as created, '
                          'updated_at as modified '
                          'FROM film_work;',
                          )
        while True:
            results = self.curs.fetchmany(self.fetch_size)
            if not results:
                break
            yield results

    def load_persons(self):
        """Загрузка данных из таблицы person.

        Returns:
            results: Массив sqlite3.Row (генератор)
        """
        self.curs.execute('SELECT id, full_name, created_at as created, updated_at as modified FROM person;')

        while True:
            results = self.curs.fetchmany(self.fetch_size)
            if not results:
                break
            yield results

    def load_genre(self):
        """Загрузка данных из таблицы genre.

        Returns:
            results: Массив sqlite3.Row (генератор)
        """
        self.curs.execute('SELECT id, name, description, created_at as created, updated_at as modified FROM genre;')

        while True:
            results = self.curs.fetchmany(self.fetch_size)
            if not results:
                break
            yield results

    def load_person_film_work(self):
        """Загрузка данных из таблицы person_film_work.

        Returns:
            results: Массив sqlite3.Row (генератор)
        """
        self.curs.execute('SELECT id, film_work_id, person_id, role, created_at as created FROM person_film_work;')

        while True:
            results = self.curs.fetchmany(self.fetch_size)
            if not results:
                break
            yield results

    def load_genre_film_work(self):
        """Загрузка данных из таблицы genre_film_work.

        Returns:
            results: Массив sqlite3.Row (генератор)
        """
        self.curs.execute('SELECT id, film_work_id, genre_id, created_at as created FROM genre_film_work;')

        while True:
            results = self.curs.fetchmany(self.fetch_size)
            if not results:
                break
            yield results
