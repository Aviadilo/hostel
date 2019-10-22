class QueryBodies:
    count_students_in_rooms = "SELECT r.id, r.name, COUNT(s.room) " \
                              "FROM rooms r " \
                              "LEFT JOIN students s ON r.id = s.room " \
                              "GROUP BY r.id " \
                              "ORDER BY r.id "

    find_the_yougest_age = "SELECT r.id, r.name, AVG(DATEDIFF(CURRENT_DATE, s.birthday)/365) AS age " \
                           "FROM rooms r " \
                           "LEFT JOIN students s ON r.id = s.room " \
                           "GROUP BY r.id " \
                           "HAVING age IS NOT NULL " \
                           "ORDER BY age " \
                           "LIMIT 5"

    find_the_biggest_age_diff = "SELECT r.id, r.name, DATEDIFF(MAX(s.birthday), MIN(s.birthday))/365 AS age_diff " \
                                " FROM rooms r " \
                                "LEFT JOIN students s ON r.id = s.room " \
                                "GROUP BY r.id " \
                                "ORDER BY age_diff DESC " \
                                "LIMIT 5"

    find_rooms_with_both_male_female = "SELECT r.id, r.name FROM rooms r " \
                                       "LEFT JOIN students s ON r.id = s.room " \
                                       "GROUP BY r.id " \
                                       "HAVING COUNT(DISTINCT s.sex) > 1 " \
                                       "ORDER BY r.id"


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
