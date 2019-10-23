import mysql.connector


class Connection:
    """Base class for creating connection to the DB"""

    def __init__(self, user_name, password, db_name):
        self.user = user_name
        self.passwd = password
        self.db_name = db_name
        self.mydb = self.connect()
        self.mycursor = self.mydb.cursor()

    def connect(self):
        mydb = mysql.connector.connect(
            host="localhost",
            user=self.user,
            passwd=self.passwd
        )
        return mydb

    def disconnect(self):
        self.mycursor.close()
        self.mydb.close()


class DBCreator(Connection):
    """Class for creating database"""

    def create(self):
        self.mycursor.execute("CREATE DATABASE {}".format(self.db_name))


class TableCreator(Connection):
    """Base class for creating table in database"""

    def create(self, name, fields):
        self.mycursor.execute("USE {}".format(self.db_name))
        self.mycursor.execute("CREATE TABLE {} ({})".format(name, fields))


class TableWriter(Connection):
    """Base class for inserting data into table"""

    def insert_data(self, value, table_name, fields):
        self.mycursor.execute("USE {}".format(self.db_name))
        operators = self.__create_operators(fields)
        sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, fields, operators)
        self.mycursor.executemany(sql, value)
        self.mydb.commit()

    def __create_operators(self, fields):
        amount = len(fields.split(","))
        operators = ",".join(['%s']*amount)
        return operators


class QueryMaker(Connection):
    """Class executes database queries"""

    def create_index(self, index_name, table_name, column_name):
        self.mycursor.execute("USE {}".format(self.db_name))
        self.mycursor.execute("CREATE INDEX {} ON {} ({})".format(index_name, table_name, column_name))
        self.mydb.commit()

    def make_query(self, query_body):
        self.mycursor.execute("USE {}".format(self.db_name))
        self.mycursor.execute(query_body)
        myresult = self.mycursor.fetchall()
        return myresult
