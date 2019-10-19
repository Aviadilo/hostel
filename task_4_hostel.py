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


class ConverterQueryToDict:
    """Class converts database queries to dictionary"""

    @staticmethod
    def convert_query_student_amount(query_list):
        query_dict = {}
        for i in query_list:
            query_dict[str(i[0])] = {'room': i[1], 'student amount': i[2]}
        return query_dict

    @staticmethod
    def convert_query_youngest_age(query_list):
        query_dict = {}
        for i in query_list:
            query_dict[str(i[0])] = {'room': i[1], 'average age': int(i[2])}
        return query_dict

    @staticmethod
    def convert_query_age_diff(query_list):
        query_dict = {}
        for i in query_list:
            query_dict[str(i[0])] = {'room': i[1], 'age difference': int(i[2])}
        return query_dict

    @staticmethod
    def convert_query_both_male_female(query_list):
        query_dict = {}
        for i in query_list:
            query_dict[str(i[0])] = {'room': i[1]}
        return query_dict


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


def create_database():
    db = DBCreator()
    db.create()
    create_tables()
    db.disconnect()


def read_files():
    rooms_path = input('Enter file name containing rooms data')
    rooms_format = input('Enter file format containing rooms data')
    students_path = input('Enter file name containing students data')
    students_format = input('Enter file format containing students data')
    rooms = ReadingData.read_file(rooms_path, rooms_format)
    students = ReadingData.read_file(students_path, students_format)
    return rooms, students


def insert_data(rooms, students):
    rooms_values = make_room_values(rooms)
    students_values = make_students_values(students)
    writing = TableWriter()
    writing.insert_data(rooms_values, 'rooms', "id, name")
    writing.insert_data(students_values, 'students', "id, name, birthday, room, sex")
    writing.disconnect()


def make_queries():
    myquery = QueryMaker()
    first = myquery.count_students_in_rooms()
    second = myquery.find_the_yougest_age()
    third = myquery.find_the_biggest_age_diff()
    fourth = myquery.find_rooms_with_both_male_female()
    myquery.disconnect()
    return first, second, third, fourth


def make_result_dictionary(first, second, third, fourth):
    query_results = {}
    query_results['students amount'] = ConverterQueryToDict.convert_query_student_amount(first)
    query_results['the youngest age'] = ConverterQueryToDict.convert_query_youngest_age(second)
    query_results['the biggest age diff'] = ConverterQueryToDict.convert_query_age_diff(third)
    query_results['both male and female'] = ConverterQueryToDict.convert_query_both_male_female(fourth)
    return query_results


def main():
    create_database()

    try:
        rooms, students = read_files()
    except FileNotFoundError:
        print("You entered incorrect file name/path/format. Please try again")
        rooms, students = read_files()

    insert_data(rooms, students)
    first, second, third, fourth = make_queries()
    query_results = make_result_dictionary(first, second, third, fourth)

    result_path = input("Enter file name with result data")
    result_format = input("Enter file format with result data")
    WritingData.write_to_file(result_path, result_format, query_results)


if __name__ == "__main__":
    main()
