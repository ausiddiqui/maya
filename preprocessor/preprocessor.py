# -*- coding: utf-8 -*-
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
        try:
            data = re.sub('\s+', ' ', data.strip()).lower()
            return data
        except AttributeError:
            return None
    """
    :param data: Data (str or unicode)
    :return: Data without the punctuation marks
    """
    @staticmethod
    def punctuation_remover(data):
        import string
        try:
            remove_punctuation_map = dict((ord(char), u' ') for char in string.punctuation)
            for i in [2404, 55357, 56842, 55356, 57198, 57252]:
                remove_punctuation_map[i] = u' '
            result = data.translate(remove_punctuation_map)
            return result
        except TypeError:
            return None

    @staticmethod
    def character_replacer(data):
        return data.replace('"', '').replace("'", "")

    @staticmethod
    def remove_special_character(data):
        char = {55357: u' ', 56842: u' ', 55356: u' ', 57198: u' ', 57252: u' '}
        result = data.translate(char)
        return result

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
    def utf8_decode(data, id):
        try:
            return data.encode('latin-1').decode('utf-8')
        except UnicodeError:
            return data
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
            return None

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
                        yield start, self.html_entity_decode(data[fields])
                    else:
                        if not self.if_in_english(data[fields]):
                            yield start, self.utf8_decode(data[fields], start)
                        else:
                            yield start, data[fields]
                except Exception:
                    yield None
            else:
                yield None
            start += 1

    @staticmethod
    def if_in_english(data):
        try:
            data.encode('ascii')
        except UnicodeEncodeError:
            return False
        except UnicodeError:
            return False
        else:
            return True


if __name__ == '__main__':
    from database import Database

    database = Database(
        'localhost',
        'nov12',
        'root',
        'root',
        'utf8mb4'
    )
    connection = database.connect_with_pymysql()
    import urllib2
    import json
    if connection:
        try:
            with connection.cursor() as cursor:
                create_schema_sql = "CREATE TABLE location(IP varchar(20) NOT NULL DEFAULT '', longitude varchar(20) DEFAULT NULL, latitude varchar(20) DEFAULT NULL, country varchar(50) DEFAULT NULL, region varchar(70) DEFAULT NULL, city varchar(40) DEFAULT NULL, zip_code varchar(20) DEFAULT NULL, PRIMARY KEY (IP)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
                cursor.execute(create_schema_sql)
                connection.commit()

                sql = "SELECT ip, forwarded FROM ips"
                cursor.execute(sql)
                data = cursor.fetchall()

                ip_set = set()

                for record in data:
                    if record['ip']:
                        parsed_ip = record['ip'].split(',')
                        for i in parsed_ip:
                            ip_set.add(i.strip())
                    if record['forwarded']:
                        parsed_ip = record['forwarded'].split(',')
                        for i in parsed_ip:
                            ip_set.add(i.strip())
                print len(ip_set)

                for i in ip_set:
                    data = urllib2.urlopen('http://freegeoip.net/json/' + i)
                    received_data = data.read()

                    if received_data:
                        data = json.loads(received_data)
                        country = data['country_code'] + ': ' + data['country_name']
                        region = (data['region_code'] + ': ' + data['region_name']).replace("'", "")
                        city = data['city'].replace("'", "")
                        latitude = data['latitude']
                        longitude = data['longitude']
                        zip_code = data['zip_code']
                    try:
                        insert_sql = "INSERT INTO location(zip_code, country, region, city, latitude, longitude, IP) VALUES('" + zip_code + "', '" + country + "', '" + region + "', '" + city + "', '" + str(
                            latitude) + "', '" + str(longitude) + "', '" + i + "')"
                        cursor.execute(insert_sql)
                        connection.commit()
                    except Exception:
                        print i
        finally:
            connection.close()
    # # decoder instance
    # decoder = Decoder()
    # if connection:
    #     try:
    #         with connection.cursor() as cursor:
    #             # example: decode all questions
    #             for data in decoder.decode_in_range(cursor, 'questions', 'body', 1, 100000):
    #                 if data:
    #                     if all(data):
    #                         try:
    #                             # example: punctuation remove
    #                             cleaned_data = Cleaner.punctuation_remover(data[1])
    #                             # example: whitespace reomve
    #                             cleaned_data = Cleaner.whitespace_remover(cleaned_data)
    #                             sql = "UPDATE questions SET body='" + cleaned_data + "' WHERE id= " + str(data[0])
    #                             cursor.execute(sql)
    #                             connection.commit()
    #                         except Exception:
    #                             print "Exception in updating id " + str(data[0])
    #     finally:
    #         connection.close()
    # from database import Database
    #
    # #connection to the database
    # # database = Database(
    # #     '<host_name>',
    # #     '<database_name>',
    # #     '<user_name>',
    # #     '<password>',
    # #     '<character_set>'
    # # )
    #
    # database = Database(
    #     'localhost',
    #     'ds_test',
    #     'root',
    #     'root',
    #     'utf8mb4'
    # )
    #
    # connection = database.connect_with_pymysql()
    #
    # # #for a successful connection
    # if connection:
    #     try:
    #         with connection.cursor() as cursor:
    #
    #
    #             # sql = "select questions.id as q_id,users.id FROM questions LEFT JOIN users ON questions.email=users.email"
    #             # cursor.execute(sql)
    #             # data = cursor.fetchall()
    #             #
    #             # for record in data:
    #             #     if record['id']:
    #             #         insert = "insert into question_user(user_id,question_id) values('"+str(record['id']) +"','"+str(record['q_id'])+"')"
    #             #         cursor.execute(insert)
    #             #         connection.commit()
    #             #     else:
    #             #         insert = "insert into question_user(question_id) values('" + str(record['q_id']) + "')"
    #             #         cursor.execute(insert)
    #             #         connection.commit()
    #             # # #decoder instance
    #             # decoder = Decoder()
    #             #
    #                          sql = "SELECT id,body,source FROM questions where status='spam'"
    #             cursor.execute(sql)
    #             data = cursor.fetchall()
    #
    #             repeat_count_a = 0
    #             repeat_count_m = 0
    #             repeat_count_w = 0
    #             repeat_count_i = 0
    #
    #             others = 0
    #
    #             for record in data:
    #                 sql = "SELECT id FROM questions WHERE id!="+str(record['id'])+" and body='"+record['body']+"'"
    #                 cursor.execute(sql)
    #                 data = cursor.fetchone()
    #                 if data:
    #                     if record['source'] == 'app':
    #                         repeat_count_a += 1
    #                     if record['source'] == 'web':
    #                         repeat_count_w += 1
    #                     if record['source'] == 'm-site':
    #                         repeat_count_m += 1
    #                     if record['source'] == 'internet.org':
    #                         repeat_count_i += 1
    #                 else:
    #                     if len(record['body']) <= 20:
    #                         if record['source'] == 'app':
    #                             repeat_count_a += 1
    #                         if record['source'] == 'web':
    #                             repeat_count_w += 1
    #                         if record['source'] == 'm-site':
    #                             repeat_count_m += 1
    #                         if record['source'] == 'internet.org':
    #                             repeat_count_i += 1
    #
    #             print 'repeat or less than 20 from app: ' + str(repeat_count_a)
    #             print 'repeat or less than 20 from web: ' + str(repeat_count_w)
    #             print 'repeat or less than 20 from m-site: ' + str(repeat_count_m)
    #             print 'repeat or less than 20 from internet: ' + str(repeat_count_i)
    #     finally:
    #         # #closing the connection
    #         connection.close()
