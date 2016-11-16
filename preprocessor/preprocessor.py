
from HTMLParser import HTMLParser
import re

"""
This class holds the necessary functions to decode and clean the encoded and raw data
Contains Cleaner and Decoder Class. And Derived MyHTMLParser class inhertied from HTMLParser Base class
"""


class Cleaner(object):
    """
    :param data: Data (str or unicode type)
    :return: data without any extra whitespace (newline, tab etc.) character
    """
    @staticmethod
    def whitespace_remover(data):
        return re.sub('\s+', ' ', data.strip())

    """
    :param data: Data (str or unicode)
    :return: Data without the punctuation marks
    """
    @staticmethod
    def punctuation_remover(data):
        import string
        remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
        return data.translate(remove_punctuation_map)

    @staticmethod
    def character_replacer(data):
        return data.replace('"', '').replace("'", "")

    @staticmethod
    def remove_special_character(data):
        import unicodedata
        return unicodedata.normalize('NFKD', data).encode('ascii', 'ignore').strip()
"""
This class derives from the base HTMLParser class
The function of the overriden handle_data function is to append the data that is within in html tags
Ending and Starting tags are being avoided by not overriding those tagas controlling methods
"""


class MyHTMLParser(HTMLParser):
    string = ''

    def handle_data(self, data):
        self.string += data

"""
Decoder class to decode
1. Html entity
2. UTF-8
From obsercation, the given task has a table that contains both html_entity_encode and utf-8 encode in different fields
The problem is we have to find out the encoding technique and decode those to get an uniform object type
We want to get unicode object for flexibility
"""


class Decoder(object):
    """
    :param data: utf-8 encoded data
    :return: unicode type decoded object or Exception
    """
    @staticmethod
    def utf8_decode(data):
        try:
            return data.encode('latin-1').decode('utf-8')
        except Exception:
            return "UnicodeError occurred " + str(Exception.message)
    """
    :param data: html_entity_encoded data
    :return: decoded data without any html tags or Exception
    """
    @staticmethod
    def html_entity_decode(data):
        html = MyHTMLParser()
        try:
            decoded_data = html.unescape(data)  # #decodes the data
            html.feed(decoded_data)  # #feeding the data to the html_parser
            return html.string  # #endoed and tag_less data
        except Exception:
            return "error"

    """
    :param connection_cursor: pymysql.connection cursor for different queries
    :param table_name: table that contains the data
    :param fields: column that contains the raw data to decode
    :param start: starting id; the range will start from this id
    :param end: the range will stop in this id (including this), if no end is provided the ending range will be the last entry
    """
    def decode_in_range(self, connection_cursor, table_name, fields, start, end=None):
        if not end:
            end = "SELECT MAX(id) FROM " + table_name
        while start <= end:
            sql = "SELECT * FROM " + table_name + " WHERE id = " + str(start)
            connection_cursor.execute(sql)
            data = connection_cursor.fetchone()
            if data:
                try:
                    if data['source'] == 'app':
                        yield self.html_entity_decode(data[fields])
                    else:
                        if not self.if_in_english(data[fields]):
                            yield self.utf8_decode(data[fields])
                        else:
                            yield data[fields]
                except Exception:
                    yield Exception.message
            else:
                yield "Nothing with id " + str(start)
            start += 1

    """
    :param connection_cursor: pymysql.connection cursor for different queries
    :param table_name: table that contains the data
    :param fields: column that contains the raw data to decode
    :param id_no: data with this id
    """
    def decode_with_id(self, connection_cursor, table_name, fields, id_no):
        sql = "SELECT * FROM " + table_name + " WHERE id = " + str(id_no)
        connection_cursor.execute(sql)
        data = connection_cursor.fetchone()
        if data:
            try:
                if data['source'] == 'app':
                    return self.html_entity_decode(data[fields])
                else:
                    if not self.if_in_english(data[fields]):
                        return self.utf8_decode(data[fields])
                    else:
                        return data[fields]
            except Exception:
                return Exception.message
                pass
        else:
            return "Nothing with id " + str(id_no)


    @staticmethod
    def if_in_english(data):
        try:
            data.encode('ascii')
        except UnicodeEncodeError:
            return False
        else:
            return True


if __name__ == '__main__':
    from database import Database

    #connection to the database
    # database = Database(
    #     '<host_name>',
    #     '<database_name>',
    #     '<user_name>',
    #     '<password>',
    #     '<character_set>'
    # )

    database = Database(
        'localhost',
        'log_user',
        'root',
        'root',
        'utf8mb4'
    )

    connection = database.connect_with_pymysql()

    # #for a successful connection
    if connection:
        try:
            with connection.cursor() as cursor:
                import urllib2
                import json

                sql = "select id,IP from logs_user"
                cursor.execute(sql)
                data = cursor.fetchall()

                for record in data:
                    data = urllib2.urlopen('http://freegeoip.net/json/'+record['IP'])
                    received_data = data.read()

                    if received_data:
                        data = json.loads(received_data)


                        country = data['country_code'] + ': ' + data['country_name']
                        region = data['region_code'] + ': ' +data['region_name']
                        city = data['city']
                        latitude = data['latitude']
                        longitude = data['longitude']
                        zip_code = data['zip_code']

                        update = "update logs_user set Zip_code='"+zip_code+"', Country='"\
                                 +country+"', Region='"+region+"', City='"+city+"', Latitude='"\
                                 +str(latitude)+"', Longitude='"+str(longitude)+"' where id="+str(record['id'])
                        cursor.execute(update)
                        connection.commit()


                # sql = "select questions.id as q_id,users.id FROM questions LEFT JOIN users ON questions.email=users.email"
                # cursor.execute(sql)
                # data = cursor.fetchall()
                #
                # for record in data:
                #     if record['id']:
                #         insert = "insert into question_user(user_id,question_id) values('"+str(record['id']) +"','"+str(record['q_id'])+"')"
                #         cursor.execute(insert)
                #         connection.commit()
                #     else:
                #         insert = "insert into question_user(question_id) values('" + str(record['q_id']) + "')"
                #         cursor.execute(insert)
                #         connection.commit()
                # # #decoder instance
                # decoder = Decoder()
                #
                # # #example: with decode_in_range
                # for data in decoder.decode_in_range(cursor, 'table_name', 'field_name', 1, 100):
                #     print data
                # # #example: with decode_by_id
                # data = decoder.decode_with_id(cursor, 'table_name', 'field_name', 99475)
                # # #example: punctuation remove
                # data = Cleaner.punctuation_remover(data)
                # # #example: whitespace reomve
                # data = Cleaner.whitespace_remover(data)
                # # #example tokenizing
                # for word in data:
                #     print word
                # sql = "SELECT id,body,source FROM questions where status='spam' and user_id is null"
                # cursor.execute(sql)
                # data = cursor.fetchall()
                #
                # repeat_count_app_user = 0
                # repeat_count_m_user = 0
                # repeat_count_web_user = 0
                # repeat_count_int_user = 0
                #
                # others = 0
                #
                # for record in data:
                #     sql = "SELECT id FROM questions WHERE id!="+str(record['id'])+" and body='"+record['body']+"'"
                #     cursor.execute(sql)
                #     data = cursor.fetchone()
                #     if data:
                #         if record['source'] == 'app':
                #             repeat_count_app_user += 1
                #         elif record['source'] == 'web':
                #             repeat_count_web_user += 1
                #         elif record['source'] == 'm-site':
                #             repeat_count_m_user += 1
                #         else:
                #             repeat_count_int_user += 1
                #     else:
                #         print str(record['id']) + '. length: ' + str(len(record['body'])) + ' :: ' + record['body']
                #
                # print ''
                # print 'app: ' + str(repeat_count_app_user)
                # print 'web: ' + str(repeat_count_web_user)
                # print 'm-site: ' + str(repeat_count_m_user)
                # print 'internet: ' + str(repeat_count_int_user)


                # print "Repeat count: " + str(repeat_count)
                # print "Others: " + str(others)
        finally:
            # #closing the connection
            connection.close()
