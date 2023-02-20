import csv
import pymysql


class PyMySQLCRUDMixin:
    def __init__(self, connection):
        self.connection = connection

    def execute_query(self, query, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
        return result

    def execute_many(self, query, params_list):
        with self.connection.cursor() as cursor:
            cursor.executemany(query, params_list)
            self.connection.commit()


class MetaTable:
    def __init__(self, connection):
        self.connection = connection
        self.crud = PyMySQLCRUDMixin(connection)

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS meta_table (
            id INT NOT NULL AUTO_INCREMENT,
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        )
        """
        self.crud.execute_query(query)

    def insert_records(self, data):
        query = """
        INSERT INTO meta_table (name) VALUES (%s)
        """
        params_list = [(d["meta_table_name"],) for d in data]
        self.crud.execute_many(query, params_list)


class FinanceTable:
    def __init__(self, connection):
        self.connection = connection
        self.crud = PyMySQLCRUDMixin(connection)

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS finance_table (
            id INT NOT NULL AUTO_INCREMENT,
            amount FLOAT NOT NULL,
            PRIMARY KEY (id)
        )
        """
        self.crud.execute_query(query)

    def insert_records(self, data):
        query = """
        INSERT INTO finance_table (amount) VALUES (%s)
        """
        params_list = [(d["finance_table_amount"],) for d in data]
        self.crud.execute_many(query, params_list)


def map_csv_to_sql_column_names(data, mapping):
    mapped_data = []
    for d in data:
        mapped_d = {sql_col: d[csv_col] for csv_col, sql_col in mapping.items()}
        mapped_data.append(mapped_d)
    return mapped_data


# connect to MySQL database
connection = pymysql.connect(
    host="localhost", user="root", password="password", db="my_database"
)

# read data from CSV file
with open("data.csv", "r") as f:
    reader = csv.DictReader(f)
    data = list(reader)

# map CSV column names to SQL column names
mapping = {"meta_table_name": "name", "finance_table_amount": "amount"}
mapped_data = map_csv_to_sql_column_names(data, mapping)

# create tables if they don't exist
meta_table = MetaTable(connection)
meta_table.create_table()
finance_table = FinanceTable(connection)
finance_table.create_table()

# insert data into tables
meta_table.insert_records(mapped_data)
finance_table.insert_records(mapped_data)

# close database connection
connection.close()
