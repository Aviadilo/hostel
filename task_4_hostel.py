from database import DBCreator, TableCreator, TableWriter, QueryMaker
from queries import QueryBodies
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
        student_fields = "id INTEGER NOT NULL, name VARCHAR(255) NOT NULL, birthday DATE, room INTEGER NOT NULL," \
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
        rooms_fields, rooms_values = self.make_fields_and_values(rooms)
        students_fields, students_values = self.make_fields_and_values(students)
        writing = TableWriter(self.user, self.passwd)
        writing.insert_data(rooms_values, 'rooms', rooms_fields)
        writing.insert_data(students_values, 'students', students_fields)
        writing.disconnect()
        self._make_queries()

    def make_fields_and_values(self, data):
        fields = ','.join(list(data[0].keys()))
        values = [tuple(i.values()) for i in data]
        return fields, values

    def _make_queries(self):
        myquery = QueryMaker(self.user, self.passwd)
        myquery.create_index('st_room', 'students', 'room')
        dict_result = {}
        for key, value in QueryBodies.query_body.items():
            result = myquery.make_query(query_body=value)
            dict_result[key] = result
        myquery.disconnect()
        self._write_data(dict_result)

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
