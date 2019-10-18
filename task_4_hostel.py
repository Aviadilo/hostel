import mysql.connector
from mysql.connector import errorcode


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
        self.disconnect()


class RoomsTableCreator(TableCreator):
    """Class for creating rooms_table in database"""
    def create_room_table(self):
        name = "rooms"
        fields = "id INTEGER NOT NULL, name VARCHAR (10), PRIMARY KEY (id)"
        # super().create(name, fields)
        self.create(name, fields)


class StudentsTableCreator(TableCreator):
    """Class for creating students_table in database"""
    def create_students_table(self):
        name = "students"
        fields = "id INTEGER NOT NULL, name VARCHAR(255) NOT NULL, birthday DATETIME, room INTEGER NOT NULL," \
                 "sex VARCHAR(1) NOT NULL, PRIMARY KEY (id), FOREIGN KEY (room) REFERENCES rooms(id) ON DELETE CASCADE"
        self.create(name, fields)


class TableWriter(Connection):
    """Base class for inserting data into table"""
    def insert_data(self, sql, value):
        self.mycursor.execute("USE hostel")
        self.mycursor.executemany(sql, value)
        self.disconnect()


def main():
    db = DBCreator()
    db.create()
    room = RoomsTableCreator()
    room.create_room_table()
    student = StudentsTableCreator()
    student.create_students_table()


if __name__ == "__main__":
    main()
