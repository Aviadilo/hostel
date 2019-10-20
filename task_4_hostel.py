from database import DBCreator, TableCreator, TableWriter, QueryMaker
from queries import QueryBodies, ConverterQueryToDict
from file_handling import DataReader, DataWriter


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
            rooms = DataReader.read_file(rooms_path, rooms_format)
            students = DataReader.read_file(students_path, students_format)
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
        first = myquery.make_query(QueryBodies.count_students_in_rooms)
        second = myquery.make_query(QueryBodies.find_the_yougest_age)
        third = myquery.make_query(QueryBodies.find_the_biggest_age_diff)
        fourth = myquery.make_query(QueryBodies.find_rooms_with_both_male_female)
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
            DataWriter.write_to_file(result_path, result_format, query_results)
        except FileNotFoundError:
            print("You entered incorrect file name/path/format. Please try again")
            self._write_data(query_results)


if __name__ == "__main__":
    start = ProgramStarter()
    start.start_program()
