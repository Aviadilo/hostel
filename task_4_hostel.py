import mysql.connector
import json
import xmltodict


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


class ProgramStarter:

    def __init__(self):
        self.user = input("Database user name")
        self.passwd = input("Database password")

    def start_program(self):
        self._create_database()

    def _create_database(self):
        db = DBCreator(self.user, self.passwd)
        db.create()
        self._create_tables()
        db.disconnect()
        self._read_files()

    def _create_tables(self):
        table = TableCreator(self.user, self.passwd)
        room_name = "rooms"
        room_fields = "id INTEGER NOT NULL, name VARCHAR (10), PRIMARY KEY (id)"
        student_name = "students"
        student_fields = "id INTEGER NOT NULL, name VARCHAR(255) NOT NULL, birthday DATETIME, room INTEGER NOT NULL," \
                         "sex VARCHAR(1) NOT NULL, PRIMARY KEY (id), FOREIGN KEY (room) REFERENCES rooms(id) ON DELETE CASCADE"
        table.create(room_name, room_fields)
        table.create(student_name, student_fields)
        table.disconnect()

    def _read_files(self):
        rooms_path = input('Enter file name containing rooms data')
        rooms_format = input('Enter file format containing rooms data')
        students_path = input('Enter file name containing students data')
        students_format = input('Enter file format containing students data')
        try:
            rooms = ReadingData.read_file(rooms_path, rooms_format)
            students = ReadingData.read_file(students_path, students_format)
            self._insert_data(rooms, students)
        except FileNotFoundError:
            print("You entered incorrect file name/path/format. Please try again")
            self._read_files()

    def _insert_data(self, rooms, students):
        rooms_values = self._make_room_values(rooms)
        students_values = self._make_students_values(students)
        writing = TableWriter(self.user, self.passwd)
        writing.insert_data(rooms_values, 'rooms', "id, name")
        writing.insert_data(students_values, 'students', "id, name, birthday, room, sex")
        writing.disconnect()
        self._make_queries()

    def _make_room_values(self, full_list):
        values = []
        for i in full_list:
            one_value = (i['id'], i['name'])
            values.append(one_value)
        return values

    def _make_students_values(self, full_list):
        values = []
        for i in full_list:
            one_value = (i['id'], i['name'], i['birthday'], i['room'], i['sex'])
            values.append(one_value)
        return values

    def _make_queries(self):
        myquery = QueryMaker(self.user, self.passwd)
        first = myquery.count_students_in_rooms()
        second = myquery.find_the_yougest_age()
        third = myquery.find_the_biggest_age_diff()
        fourth = myquery.find_rooms_with_both_male_female()
        myquery.disconnect()
        self._make_result_dictionary(first, second, third, fourth)

    def _make_result_dictionary(self, first, second, third, fourth):
        query_results = {}
        query_results['students amount'] = ConverterQueryToDict.convert_query_student_amount(first)
        query_results['the youngest age'] = ConverterQueryToDict.convert_query_youngest_age(second)
        query_results['the biggest age diff'] = ConverterQueryToDict.convert_query_age_diff(third)
        query_results['both male and female'] = ConverterQueryToDict.convert_query_both_male_female(fourth)
        self._write_data(query_results)

    def _write_data(self, query_results):
        result_path = input("Enter file name with result data")
        result_format = input("Enter file format with result data")
        try:
            WritingData.write_to_file(result_path, result_format, query_results)
        except FileNotFoundError:
            print("You entered incorrect file name/path/format. Please try again")
            self._write_data(query_results)


if __name__ == "__main__":
    start = ProgramStarter()
    start.start_program()
