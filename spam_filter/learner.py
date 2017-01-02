# -*- coding: utf-8 -*-
from database_connection import local_connection
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.externals import joblib
import numpy as np


def build_data_frame(record):
    train_target = np.array([])
    train_target_names = []
    train_data = []

    for i in record:
        train_target_names.append(i['status'])
        train_data.append(i['body'])
        if i['status'] == 'ham':
            train_target = np.append(train_target, 1)
        else:
            train_target = np.append(train_target, 0)
    return train_data, train_target


def get_questions(connection_cursor):
    sql = "SELECT questions_id,body,status FROM filtered_questions"
    connection_cursor.execute(sql)
    return connection_cursor.fetchall()


if __name__ == '__main__':
    connection = local_connection('spam_filter')
    if connection:
        try:
            with connection.cursor() as cursor:
                sql_data = get_questions(cursor)
                train_data, train_target = build_data_frame(sql_data)
                pipeline = Pipeline([('vect', CountVectorizer(max_df=0.8)),
                                     ('tfidf', TfidfTransformer()),
                                     ('clf', MultinomialNB(fit_prior=False)),
                                     ])
                pipeline = pipeline.fit(train_data, train_target)
                joblib.dump(pipeline, 'learner.pkl')
        finally:
            connection.close()