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