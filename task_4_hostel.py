import mysql.connector
import json
import xmltodict


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

    def insert_data(self, value, table_name, fields):
        operators = self.__create_operators(fields)
        self.mycursor.execute("USE hostel")
        sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name, fields, operators)
        self.mycursor.executemany(sql, value)
        self.mydb.commit()

    def __create_operators(self, fields):
        amount = len(fields.split(","))
        operators = "%s,"*amount
        operators = operators[:-1]
        return operators


class QueryMaker(Connection):
    """Class executes database queries"""

    def count_students_in_rooms(self):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute("SELECT * FROM students")
        myresult = self.mycursor.fetchall()
        return myresult

    def find_the_yougest_age(self):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute("SELECT * FROM students")
        myresult = self.mycursor.fetchall()
        return myresult

    def find_the_biggest_age_diff(self):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute("SELECT * FROM students")
        myresult = self.mycursor.fetchall()
        return myresult

    def find_rooms_with_both_male_female(self):
        self.mycursor.execute("USE hostel")
        self.mycursor.execute("SELECT * FROM students")
        myresult = self.mycursor.fetchall()
        return myresult


class ReadingData:
    """Read data from file with specified format"""

    @staticmethod
    def read_file(file_name, file_format):
        with open('{}.{}'.format(file_name, file_format), 'r') as f:
            text_file = json.load(f)
        return text_file


class WritingData():
    """Write data to file with specified format"""

    @staticmethod
    def write_to_file(file_name, file_format, text):
        if file_format.lower() == 'xml':
            WritingData.write_to_file_xml(file_name, file_format, text)
        else:
            WritingData.write_to_file_json(file_name, file_format, text)

    @staticmethod
    def write_to_file_json(file_name, file_format, text):
        with open('{}.{}'.format(file_name, file_format), 'w') as f:
            json.dump(text, f)

    @staticmethod
    def write_to_file_xml(file_name, file_format, text):
        dict_text = {'root': text}
        xml_text = xmltodict.unparse(dict_text, pretty=True, full_document=False)
        with open('{}.{}'.format(file_name, file_format), 'w') as f:
            f.write(xml_text)


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


def make_room_values(full_list):
    values = []
    for i in full_list:
        one_value = (i['id'], i['name'])
        values.append(one_value)
    return values


def make_students_values(full_list):
    values = []
    for i in full_list:
        one_value = (i['id'], i['name'], i['birthday'], i['room'], i['sex'])
        values.append(one_value)
    return values


def main():
    # create database and tables
    db = DBCreator()
    db.create()
    create_tables()
    db.disconnect()

    # read files and insert data into tables
    rooms = ReadingData.read_file('rooms', 'json')
    students = ReadingData.read_file('students', 'json')

    rooms_values = make_room_values(rooms)
    students_values = make_students_values(students)

    writing = TableWriter()
    writing.insert_data(rooms_values, 'rooms', "id, name")
    writing.insert_data(students_values, 'students', "id, name, birthday, room, sex")
    writing.disconnect()

    # make queries
    query_results = {}
    myquery = QueryMaker()
    query_results['the youngest age'] = myquery.find_the_yougest_age()
    query_results['the biggest age diff'] = myquery.find_the_biggest_age_diff()
    query_results['both male and female'] = myquery.find_rooms_with_both_male_female()
    query_results['all rooms'] = myquery.count_students_in_rooms()
    myquery.disconnect()

    # write results to file
    WritingData.write_to_file('query_results', 'json', query_results)


if __name__ == "__main__":
    main()
