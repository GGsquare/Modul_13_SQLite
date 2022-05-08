import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

class TodosSQLite:

#    """Tworzenie tabel"""

    def execute_sql(conn, sql):
        """ Execute sql
        :param conn: Connection object
        :param sql: a SQL script
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(sql)
        except Error as e:
            print(e)

#    """Dodawanie danych"""

    def add_projekt(conn, projekt):
        """
        Create a new projekt into the projects table
        :param conn:
        :param projekt:
        :return: projekt id
        """
        sql = '''INSERT INTO projects(nazwa, start_date, end_date)
                  VALUES(?,?,?)'''
        cur = conn.cursor()
        cur.execute(sql, projekt)
        conn.commit()
        return cur.lastrowid

    def add_zadanie(conn, zadanie):
        """
        Create a new zadanie into the tasks table
        :param conn:
        :param zadanie:
        :return: zadanie id
        """
        sql = '''INSERT INTO tasks(projekt_id, nazwa, opis, status, start_date, end_date)
                  VALUES(?,?,?,?,?,?)'''
        cur = conn.cursor()
        cur.execute(sql, zadanie)
        conn.commit()
        return cur.lastrowid
     
#    """Pobieranie danych"""

    """conn = create_connection("database.db")
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks")"""
    """Pobieranie wszystkich"""
    """rows = cur.fetchall()
    rows"""
    """Pobieranie jednego"""
    """rows = cur.fetchone()
    rows = cur.fetchone()
    rows"""
    """...itd.itd...."""

    def select_zadanie_by_status(conn, status):
        """
        Query tasks by priority
        :param conn: the Connection object
        :param status:
        :return:
        """
        cur = conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE status=?", (status,))
     
        rows = cur.fetchall()
        return rows
    
    def select_all(conn, table):
        """
        Query all rows in the table
        :param conn: the Connection object
        :return:
        """
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        return rows

    def new_method(conn):
        cur = conn.cursor()
        return cur

    def select_where(conn, table, **query):
        """
        Query tasks from table with data from **query dict
        :param conn: the Connection object
        :param table: table name
        :param query: dict of attributes and values
        :return:
        """
        cur = conn.cursor()
        qs = []
        values = ()
        for k, v in query.items():
            qs.append(f"{k}=?")
            values += (v,)
        q = " AND ".join(qs)
        cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
        rows = cur.fetchall()
        return rows

#   """Pobieranie projektó i zadań"""

    conn = create_connection("database.db")
    """Wszystkie projekty, wszystkie zadania"""
    select_all(conn, "projects")
    select_all(conn, "tasks")
    """Wszystkie zadania dla projektu o id 1"""
    select_where(conn, "tasks", projekt_id=1)
    """Wszystkie zadania ze statusem ended"""
    select_where(conn, "tasks", status="ended")

#    """Aktualizacja danych"""

    def update(conn, table, id, **kwargs):
        """
        update status, begin_date, and end date of a task
        :param conn:
        :param table: table name
        :param id: row id
        :return:
        """
        parameters = [f"{k} = ?" for k in kwargs]
        parameters = ", ".join(parameters)
        values = tuple(v for v in kwargs.values())
        values += (id, )
     
        sql = f''' UPDATE {table}
                  SET {parameters}
                  WHERE id = ?'''
        try:
            cur = conn.cursor()
            cur.execute(sql, values)
            conn.commit()
            print("OK")
        except sqlite3.OperationalError as e:
            print(e)

#    """Usuwanie danych"""

    def delete_where(conn, table, **kwargs):
       """
       Delete from table where attributes from
       :param conn:  Connection to the SQLite database
       :param table: table name
       :param kwargs: dict of attributes and values
       :return:
       """
       qs = []
       values = tuple()
       for k, v in kwargs.items():
           qs.append(f"{k}=?")
           values += (v,)
       q = " AND ".join(qs)
    
       sql = f'DELETE FROM {table} WHERE {q}'
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("Deleted")

    def delete_all(conn, table):
        """
        Delete all rows from table
        :param conn: Connection to the SQLite database
        :param table: table name
        :return:
        """
        sql = f'DELETE FROM {table}'
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        print("Deleted")
     
    if __name__ == "__main__":
        conn = create_connection("database.db")
        update(conn, "tasks", 2, status="started")
        update(conn, "tasks", 2, stat="ended")
        delete_where(conn, "tasks", id=3)
        delete_all(conn, "tasks")

#    """Dodawanie konkretnego projektu i zadania"""

        projekt = ("Zadanie z modułu 13", "2022-05-03 08:00:00", "2022-05-03 12:00:00")
        projekt = ("Zaległe zadania z poprzednich modułów", "2022-05-03 00:00:00", "2022-05-03 23:00:00")
        pr_id = add_projekt(conn, projekt)

        zadanie = (
        pr_id,
            "Zamówić dźwig",
            "Rozformowanie kany na 06/05 na godzine 10:00",
            "started",
            "2022-05-03 07:00:00",
            "2020-05-03 07:05:00"
        )
        zadanie = (
        pr_id,
            "Moduł 7",
            "Dokończyć zadanie",
            "started",
            "2022-05-02 07:00:00",
            "2022-05-03 23:00:00"
        )
        zadanie_id = add_zadanie(conn, zadanie)
        print(pr_id, zadanie_id)
        conn.commit()

        conn.close()