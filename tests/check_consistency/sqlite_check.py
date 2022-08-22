"""Основной модель для запроса данных из таблиц SQLite."""

import sqlite3


class SQLiteChecker:
    def __init__(self, connection: sqlite3.Connection):
        """Объект для коннекта к SQLite.

        Args:
            connection (sqlite3.Connection): коннект к SQLite
        """
        self.connection: sqlite3.Connection = connection
        self.connection.row_factory = sqlite3.Row
        self.curs = self.connection.cursor()

    def load_table_row_count(self, table):
        """Метод для получения количества строк из тбалицы.

        Args:
            table (str): таблица, к которой обращаемся

        Returns:
            row number

        Raises:
            Exception: если указать таблицу не из списка
        """
        if table not in ['film_work', 'genre_film_work', 'person_film_work', 'genre', 'person']:
            raise Exception("Не указана теблица или такой таблицы нет в списке"
                            "film_work, genre_film_work, person_film_work, genre, person")

        self.curs.execute(f"SELECT count(*) as count from {table}")
        return dict(self.curs.fetchall()[0]).get('count')

    def load_table_to_campare(self, table: str, limit: int = 100, offset: int = 0):
        """Возвращает данные из указанной таблицы. Можно грузить пачками.

        Args:
            table: таблица из которой грузим данные
            limit: количество строк для загрузки
            offset: сдвиг для загрузки пачками

        Returns:
            массив строк

        Raises:
            Exception: если указать таблицу не из списка
        """
        if table not in ['film_work', 'genre_film_work', 'person_film_work', 'genre', 'person']:
            raise Exception("Не указана теблица или такой таблицы нет в списке"
                            "film_work, genre_film_work, person_film_work, genre, person")

        if table == 'film_works':
            self.curs.execute(f"SELECT id"
                              f", title"
                              f", description"
                              f", substr(creation_date,1,10)"
                              f", rating"
                              f", type"
                              f", substr(created_at,1,19)"
                              f", substr(updated_at,1,19)"
                              f" from {table} order by id limit {limit} offset {offset};",
                              )
        elif table == 'person':
            self.curs.execute(f"SELECT id"
                              f", full_name"
                              f", substr(created_at,1,19)"
                              f", substr(updated_at,1,19)"
                              f" from {table} order by id limit {limit} offset {offset};",
                              )
        elif table == 'genre':
            self.curs.execute(f"SELECT id"
                              f", name"
                              f", description"
                              f", substr(created_at,1,19)"
                              f", substr(updated_at,1,19)"
                              f" from {table} order by id limit {limit} offset {offset};",
                              )
        elif table == 'genre_film_work':
            self.curs.execute(f"SELECT id"
                              f", film_work_id"
                              f", genre_id"
                              f", substr(created_at,1,19)"
                              f" from {table} order by id limit {limit} offset {offset};",
                              )
        elif table == 'person_film_work':
            self.curs.execute(f"SELECT id"
                              f", film_work_id"
                              f", person_id"
                              f", role"
                              f", substr(created_at,1,19)"
                              f" from {table} order by id limit {limit} offset {offset};",
                              )

        return [', '.join(map(str, [_ for _ in dict(_).values()])) for _ in self.curs.fetchall()]
