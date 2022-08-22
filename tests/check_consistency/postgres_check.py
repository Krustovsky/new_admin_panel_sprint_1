"""Основной модель для запроса данных из таблиц Postgres."""


class PostgresChecker:
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

    def load_table_count(self, table: str):
        """Метод для получения количества строк из тбалицы.

        Args:
            table (str): таблица, к которой обращаемся

        Returns:
            row number

        Raises:
            Exception: если указать таблицу не из списка
        """
        if table not in ['film_work', 'genre_film_work', 'person_film_work', 'genre', 'person']:
            raise Exception('Не указана теблица или такой таблицы нет в списке'
                            'film_work, genre_film_work, person_film_work, genre, person')
        self.cur.execute(f'SELECT count(*) from {self.schema}{table};')
        return self.cur.fetchall()[0][0]

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
            raise Exception('Не указана теблица или такой таблицы нет в списке'
                            'film_work, genre_film_work, person_film_work, genre, person')

        if table == 'film_works':
            self.cur.execute(f"SELECT id"
                             f", title"
                             f", description"
                             f", to_char(creation_date, 'YYYY-MM-DD') as creation_date"
                             f", rating"
                             f", type"
                             f", to_char(created, 'YYYY-MM-DD HH24:MI:SS') as created"
                             f", to_char(modified, 'YYYY-MM-DD HH24:MI:SS') as modified"
                             f" from {self.schema}{table} order by id limit {limit} offset {offset};",
                             )

        elif table == 'person':
            self.cur.execute(f"SELECT id"
                             f", full_name"
                             f", to_char(created, 'YYYY-MM-DD HH24:MI:SS') as created"
                             f", to_char(modified, 'YYYY-MM-DD HH24:MI:SS') as modified"
                             f" from {self.schema}{table} order by id limit {limit} offset {offset};",
                             )
        elif table == 'genre':
            self.cur.execute(f"SELECT id"
                             f", name"
                             f", description"
                             f", to_char(created, 'YYYY-MM-DD HH24:MI:SS') as created"
                             f", to_char(modified, 'YYYY-MM-DD HH24:MI:SS') as modified"
                             f" from {self.schema}{table} order by id limit {limit} offset {offset};",
                             )

        elif table == 'genre_film_work':
            self.cur.execute(f"SELECT id"
                             f", film_work_id"
                             f", genre_id"
                             f", to_char(created, 'YYYY-MM-DD HH24:MI:SS') as created"
                             f" from {self.schema}{table} order by id limit {limit} offset {offset};",
                             )

        elif table == 'person_film_work':
            self.cur.execute(f"SELECT id"
                             f", film_work_id"
                             f", person_id"
                             f", role"
                             f", to_char(created, 'YYYY-MM-DD HH24:MI:SS') as created"
                             f" from {self.schema}{table} order by id limit {limit} offset {offset};",
                             )

        return [', '.join(map(str, [_ for _ in dict(_).values()])) for _ in self.cur.fetchall()]
