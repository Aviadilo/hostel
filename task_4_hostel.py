import mysql.connector


class Connection:
    """Base class for creating connection to the DB"""

    def __init__(self):
        self.mydb = self.connect()
        self.mycursor = self.mydb.cursor()

    def connect(self):
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="anna",
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

    def insert_data(self, sql, value):
        self.mycursor.execute("USE hostel")
        self.mycursor.executemany(sql, value)


def create_tables():
    table = TableCreator()
    room_name = "rooms"
    room_fields = "id INTEGER NOT NULL, name VARCHAR (10), PRIMARY KEY (id)"
    student_name = "students"
    student_fields = "id INTEGER NOT NULL, name VARCHAR(255) NOT NULL, birthday DATETIME, room INTEGER NOT NULL," \
                     "sex VARCHAR(1) NOT NULL, PRIMARY KEY (id), FOREIGN KEY (room) REFERENCES rooms(id) ON DELETE CASCADE"
    table.create(room_name, room_fields)
    table.create(student_name, student_fields)
    table.disconnect()


def main():
    db = DBCreator()
    db.create()
    create_tables()


if __name__ == "__main__":
    main()
