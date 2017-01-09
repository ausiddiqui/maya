#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
import os
from sklearn.externals import joblib
from preprocessor import Cleaner, Decoder
from database_connection import local_connection
from difflib import SequenceMatcher
from word_list import abusive_words, abusive_words_bangla
import logging
from logging.config import dictConfig

SIMILARITY_COEFFICIENT = 0.8
MINIMUM_NUMBER_OF_WORDS = 10
WORD_PER_PROBABILITY = 0.3
ABUSIVE_WORDS_PROBABILITY = 0.15
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGGING_DIR = os.path.join(BASE_DIR+'/spam_filter/', 'logging.json')


class BasicFilter:
    def __init__(self):
        with open(LOGGING_DIR) as data_file:
            data = json.load(data_file)

        dictConfig(data)
        self.logger = logging.getLogger()

    @staticmethod
    def similar(a, b):
        return SequenceMatcher(None, a, b).ratio()

    def clean_data(self, question):
        decoder = Decoder()
        try:
            data = Cleaner.whitespace_remover(Cleaner.punctuation_remover(Cleaner.remove_num(decoder.decode(question))))
            question['body'] = data
        except Exception:
            self.logger.error('Exception in cleaning data', exc_info=True)
        return question

    def get_last_question(self, connection_cursor, question):
        sql = "SELECT body FROM questions WHERE email='" + question['email'] + "' and source='" + \
              question['source'] + "' and id<'" + str(question['id']) + "' ORDER BY id desc"
        connection_cursor.execute(sql)
        return connection_cursor.fetchone()

    def get_question_with_id(self, connection_cursor, id):
        sql = "SELECT * FROM questions WHERE id='" + str(id) + "'"
        connection_cursor.execute(sql)
        return connection_cursor.fetchone()

    def is_repeat(self, connection_cursor, question):
        try:
            sql_data = self.get_last_question(connection_cursor, question)
            if sql_data:
                if self.similar(sql_data['body'], question['body']) > SIMILARITY_COEFFICIENT:
                    return True
            else:
                return False
        except Exception:
            self.logger.error('Exception in finding repeat', exc_info=True)
            return False
        return False

    def basic_filter(self, question):
        question = json.loads(question)
        # 0, 1 = Spam, Ham
        # importing the learner
        clf = joblib.load(os.path.join(BASE_DIR+'/spam_filter/', 'learner.pkl'))
        # establishing the connection to the database
        connection = local_connection('spam_filter')
        if connection:
            try:
                with connection.cursor() as cursor:
                    if self.is_repeat(cursor, question):
                        return json.dumps({'status': 0})
                    else:
                        qs = self.clean_data(question)
                        if qs['body'] == '':
                            return json.dumps({'status': 0})
                        else:
                            prediction = clf.predict_proba([qs['body']])
                            question_as_list = qs['body'].split(' ')
                            weight = WORD_PER_PROBABILITY * MINIMUM_NUMBER_OF_WORDS -\
                                     (len(question_as_list) * WORD_PER_PROBABILITY)
                            if prediction[0][0] <= 0.5 and weight > 0:
                                prediction[0][0] += weight
                                prediction[0][1] -= weight
                            for words in abusive_words+abusive_words_bangla:
                                if words in question_as_list:
                                    if prediction[0][0] <= 0.5:
                                        prediction[0][0] += ABUSIVE_WORDS_PROBABILITY
                                        prediction[0][1] -= ABUSIVE_WORDS_PROBABILITY
                                    else:
                                        break
                            if prediction[0][0] > prediction[0][1]:
                                return json.dumps({'status': 0})
                    return json.dumps({'status': 1})
            except Exception:
                self.logger.error('Exception in getting probability', exc_info=True)
                return json.dumps({'status': 1})
            finally:
                connection.close()

if __name__ == '__main__':
    new_filter = BasicFilter()
    print new_filter.basic_filter(sys.argv[1])
    # connection = local_connection('spam_filter')
    # print json.dumps(new_filter.get_question_with_id(connection.cursor(), 33), default=lambda x: str(x))
