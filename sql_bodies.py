class QueryBodies:
    query_body = {
        'count_students_in_rooms' : "SELECT r.id, r.name, COUNT(s.room) "
                                    "FROM rooms r "
                                    "LEFT JOIN students s ON r.id = s.room "
                                    "GROUP BY r.id "
                                    "ORDER BY r.id",

        'the_youngest_age' : "SELECT r.id, r.name, AVG(DATEDIFF(CURRENT_DATE, s.birthday)) AS age "
                             "FROM rooms r LEFT JOIN students s ON r.id = s.room "
                             "GROUP BY r.id "
                             "HAVING age IS NOT NULL "
                             "ORDER BY age "
                             "LIMIT 5",

        'find_the_biggest_age_diff' : "SELECT r.id, r.name, DATEDIFF(MAX(s.birthday), MIN(s.birthday)) AS age "
                                      "FROM rooms r "
                                      "LEFT JOIN students s ON r.id = s.room "
                                      "GROUP BY r.id "
                                      "ORDER BY age DESC "
                                      "LIMIT 5",

        'find_rooms_with_both_male_female' : "SELECT r.id, r.name "
                                             "FROM rooms r "
                                             "LEFT JOIN students s ON r.id = s.room "
                                             "GROUP BY r.id "
                                             "HAVING COUNT(DISTINCT s.sex) > 1 "
                                             "ORDER BY r.id"
    }
