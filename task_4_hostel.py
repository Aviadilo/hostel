from .database import DBCreator, TableCreator, TableWriter, QueryMaker
from .sql_bodies import QueryBodies, TableFieldsBodies
from .file_handling import DataReader, DataWriter


class ProgramStarter:

    def __init__(self):
        self.user = input("Database user name")
        self.passwd = input("Database password")
        self.db_name = input("Database name")

    def start_program(self):
        self._create_database()

    def _create_database(self):
        db = DBCreator(self.user, self.passwd, self.db_name)
        db.create()
        db.disconnect()
        self._create_tables()

    def _create_tables(self):
        table = TableCreator(self.user, self.passwd, self.db_name)
        for key, value in TableFieldsBodies.field.items():
            table.create(key, value)
        table.disconnect()
        self._read_files()

    def _read_files(self):
        rooms_path = input('Enter file name containing rooms data')
        rooms_format = input('Enter file format containing rooms data')
        students_path = input('Enter file name containing students data')
        students_format = input('Enter file format containing students data')
        try:
            data_to_insert = {}
            data_to_insert['rooms'] = DataReader.read_file(rooms_path, rooms_format)
            data_to_insert['students'] = DataReader.read_file(students_path, students_format)
            self._insert_data(data_to_insert)
        except FileNotFoundError:
            print("You entered incorrect file name/path/format. Please try again")
            self._read_files()

    def _insert_data(self, data_to_insert):
        writing = TableWriter(self.user, self.passwd, self.db_name)
        for key, value in data_to_insert.items():
            fields, values = self._make_fields_and_values(value)
            writing.insert_data(values, key, fields)
        writing.disconnect()
        self._make_queries()

    def _make_fields_and_values(self, data):
        fields = ','.join(list(data[0].keys()))
        values = [tuple(i.values()) for i in data]
        return fields, values

    def _make_queries(self):
        myquery = QueryMaker(self.user, self.passwd, self.db_name)
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
