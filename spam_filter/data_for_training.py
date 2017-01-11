from database_connection import local_connection
from preprocessor import Decoder, Cleaner


def clean_questions(connection_cursor, connection_instance, decoder_instance):
    end = "SELECT MAX(id) FROM questions"
    for question_body in decoder_instance.decode_in_range(connection_cursor, 1, end):
        if question_body:
            if all(question_body):
                try:
                    # example: punctuation remove
                    cleaned_data = Cleaner.punctuation_remover(question_body[1])
                    # example: whitespace reomve
                    cleaned_data = Cleaner.remove_num(Cleaner.whitespace_remover(cleaned_data))
                    update_questions(connection_cursor, connection_instance, cleaned_data, str(question_body[0]))
                except Exception:
                    print "Exception in updating id " + str(question_body[0])


def update_questions(connection_cursor, connection_instance, cleaned_data, id):
    sql = "UPDATE questions SET body='" + cleaned_data + "' WHERE id= " + id
    connection_cursor.execute(sql)
    connection_instance.commit()


def select_questions(connection_cursor):
    select_sql = "SELECT id,body,status FROM questions"
    connection_cursor.execute(select_sql)
    return connection_cursor.fetchall()


def match_question(connection_cursor, id, body):
    match_sql = "SELECT id FROM questions WHERE id<" + id + " and body ='" + body + "'"
    connection_cursor.execute(match_sql)
    return connection_cursor.fetchone()


def insert_spam_ham(connection_instance, connection_cursor, id, body, status):
    insert_sql = "INSERT INTO filtered_questions(questions_id, body, status) " \
                 "VALUES('" + id + "','" + body + "','" + status + "')"
    connection_cursor.execute(insert_sql)
    connection_instance.commit()


if __name__ == '__main__':
    connection = local_connection('spam_filter')
    # decoder instance
    decoder = Decoder()

    if connection:
        try:
            with connection.cursor() as cursor:
                clean_questions(cursor, connection, decoder)
                data = select_questions(cursor)
                for i in data:
                    is_matched = match_question(cursor, str(i['id']), i['body'])

                    if not is_matched:
                        if i['status'] != 'spam':
                            i['status'] = 'ham'
                        insert_spam_ham(connection, cursor, str(i['id']), i['body'], i['status'])
        finally:
            connection.close()