import mysql.connector


class Connection:
    """Base class for creating connection to the DB"""

    def __init__(self, user_name, password):
        self.user = user_name
        self.passwd = password
        self.mydb = self.connect()
        self.mycursor = self.mydb.cursor()

    def connect(self):
        mydb = mysql.connector.connect(
            host="localhost",
            user=self.user,
            passwd=self.passwd,
        )
        return mydb

    def disconnect(self):
        self.mycursor.close()
        self.mydb.close()


class DBCreator(Connection):
    """Class for creating database"""

    def create(self):
        self.mycursor.execute("CREATE DATABASE hostel")
        self.mydb.close()


class TableCreator(Connection):
    """Base class for creating table in database"""

    def create(self, name, fields):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute("CREATE TABLE {} ({})".format(name, fields))


class TableWriter(Connection):
    """Base class for inserting data into table"""

    def insert_data(self, value, table_name, fields):
        operators = self.__create_operators(fields)
        self.mycursor.execute("USE hostel")
        sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, fields, operators)
        self.mycursor.executemany(sql, value)
        self.mydb.commit()

    def __create_operators(self, fields):
        amount = len(fields.split(","))
        operators = "%s," * amount
        operators = operators[:-1]
        return operators


class QueryMaker(Connection):
    """Class executes database queries"""

    def count_students_in_rooms(self):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute(
            "SELECT r.id, r.name, COUNT(s.room) "
            "FROM rooms r "
            "LEFT JOIN students s ON r.id = s.room "
            "GROUP BY s.room "
            "ORDER BY s.room "
        )
        myresult = self.mycursor.fetchall()
        return myresult

    def find_the_yougest_age(self):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute(
            "SELECT r.id, r.name, "
            "AVG((YEAR(CURRENT_TIMESTAMP(0)) - YEAR(s.birthday)) - "
            "(RIGHT(CURRENT_TIMESTAMP(0),5)<RIGHT(s.birthday,5))) AS age "
            "FROM rooms r "
            "LEFT JOIN students s ON r.id = s.room "
            "GROUP BY s.room "
            "HAVING age IS NOT NULL "
            "ORDER BY age "
            "LIMIT 5"
        )
        myresult = self.mycursor.fetchall()
        return myresult

    def find_the_biggest_age_diff(self):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute(
            "SELECT r.id, r.name, (MAX(YEAR(s.birthday))-MIN(YEAR(s.birthday))) AS age_diff "
            " FROM rooms r "
            "LEFT JOIN students s ON r.id = s.room "
            "GROUP BY s.room "
            "ORDER BY age_diff DESC "
            "LIMIT 5"
        )
        myresult = self.mycursor.fetchall()
        return myresult

    def find_rooms_with_both_male_female(self):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute(
            "SELECT r.id, r.name FROM rooms r "
            "LEFT JOIN students s ON r.id = s.room "
            "WHERE s.sex IN ('F','M') "
            "GROUP BY s.room "
            "HAVING COUNT(DISTINCT s.sex) = 2 "
            "ORDER BY s.room"
        )
        myresult = self.mycursor.fetchall()
        return myresult
