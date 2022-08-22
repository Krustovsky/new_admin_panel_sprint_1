"""Модуль для загрузки данных из таблиц SQLite."""
import sqlite3


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        """Объект для коннекта к SQLite.

        Args:
            connection (sqlite3.Connection): коннект к SQLite
        """
        self.connection: sqlite3.Connection = connection
        self.connection.row_factory = sqlite3.Row
        self.curs = self.connection.cursor()

    def load_movies(self):
        """Загрузка данных из таблицы film_work.

        Returns:
            fetchall(): Массив sqlite3.Row
        """
        self.curs.execute('SELECT id, title, description, '
                          'creation_date, rating, type, created_at as created, '
                          'updated_at as modified '
                          'FROM film_work;',
                          )

        return self.curs.fetchall()

    def load_persons(self):
        """Загрузка данных из таблицы person.

        Returns:
            fetchall(): Массив sqlite3.Row
        """
        self.curs.execute('SELECT id, full_name, created_at as created, updated_at as modified FROM person;')

        return self.curs.fetchall()

    def load_genre(self):
        """Загрузка данных из таблицы genre.

        Returns:
            fetchall(): Массив sqlite3.Row
        """
        self.curs.execute('SELECT id, name, description, created_at as created, updated_at as modified FROM genre;')

        return self.curs.fetchall()

    def load_person_film_work(self):
        """Загрузка данных из таблицы person_film_work.

        Returns:
            fetchall(): Массив sqlite3.Row
        """
        self.curs.execute('SELECT id, film_work_id, person_id, role, created_at as created FROM person_film_work;')

        return self.curs.fetchall()

    def load_genre_film_work(self):
        """Загрузка данных из таблицы genre_film_work.

        Returns:
            fetchall(): Массив sqlite3.Row
        """
        self.curs.execute('SELECT id, film_work_id, genre_id, created_at as created FROM genre_film_work;')

        return self.curs.fetchall()
