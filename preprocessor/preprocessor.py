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
        if isinstance(data, str):
            data = unicode(data)
        remove_punctuation_map = dict((ord(char), None) for char in string.punctuation)
        return data.translate(remove_punctuation_map)

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
            return unicode(data.encode('latin-1'))
        except UnicodeError:
            return "UnicodeError occurred " + str(UnicodeError.message)

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
                    elif data['source'] == 'web':
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
                elif data['source'] == 'web':
                    return self.utf8_decode(data[fields])
                else:
                    return data[fields]
            except Exception:
                return Exception.message
        else:
            return "Nothing with id " + str(id_no)


if __name__ == '__main__':
    from database import Database

    # #connection to the database
    database = Database(
        '<host_name>',
        '<database_name>',
        '<user_name>',
        '<password>',
        '<character_set>'
    )
    connection = database.connect_with_pymysql()

    # #for a successful connection
    if connection:
        try:
            with connection.cursor() as cursor:
                # #decoder instance
                decoder = Decoder()

                # #example: with decode_in_range
                for data in decoder.decode_in_range(cursor, 'table_name', 'field_name', 1, 100):
                    print data
                # #example: with decode_by_id
                data = decoder.decode_with_id(cursor, 'table_name', 'field_name', 99475)
                # #example: punctuation remove
                data = Cleaner.punctuation_remover(data)
                # #example: whitespace reomve
                data = Cleaner.whitespace_remover(data)
                # #example tokenizing
                for word in data:
                    print word
        finally:
            # #closing the connection
            connection.close()