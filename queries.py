class QueryBodies:
    count_students_in_rooms = "SELECT r.id, r.name, COUNT(s.room) " \
                              "FROM rooms r " \
                              "LEFT JOIN students s ON r.id = s.room " \
                              "GROUP BY s.room " \
                              "ORDER BY s.room "

    find_the_yougest_age = "SELECT r.id, r.name, " \
                           "AVG((YEAR(CURRENT_TIMESTAMP(0)) - YEAR(s.birthday)) - " \
                           "(RIGHT(CURRENT_TIMESTAMP(0),5)<RIGHT(s.birthday,5))) AS age " \
                           "FROM rooms r " \
                           "LEFT JOIN students s ON r.id = s.room " \
                           "GROUP BY s.room " \
                           "HAVING age IS NOT NULL " \
                           "ORDER BY age " \
                           "LIMIT 5"

    find_the_biggest_age_diff = "SELECT r.id, r.name, (MAX(YEAR(s.birthday))-MIN(YEAR(s.birthday))) AS age_diff " \
                                " FROM rooms r " \
                                "LEFT JOIN students s ON r.id = s.room " \
                                "GROUP BY s.room " \
                                "ORDER BY age_diff DESC " \
                                "LIMIT 5"

    find_rooms_with_both_male_female = "SELECT r.id, r.name FROM rooms r " \
                                       "LEFT JOIN students s ON r.id = s.room " \
                                       "WHERE s.sex IN ('F','M') " \
                                       "GROUP BY s.room " \
                                       "HAVING COUNT(DISTINCT s.sex) = 2 " \
                                       "ORDER BY s.room"


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
