if __name__ == '__main__':
    from database import Database
    from preprocessor import Cleaner
    from sklearn.feature_extraction.text import CountVectorizer

    database = Database(
        'localhost',
        'DS_test',
        'root',
        'root',
        'utf8mb4'
    )

    connection = database.connect_with_pymysql()

    # #for a successful connection
    if connection:
        try:
            with connection.cursor() as cursor:
                get_spams_sql = "SELECT body from questions where status='spam'"
                cursor.execute(get_spams_sql)
                spam_data = cursor.fetchall()
        finally:
            connection.close()

    corpus = []
    if spam_data:
        for spam in spam_data:
            corpus.append(Cleaner.punctuation_remover(spam['body']))

        del spam_data

    # if corpus:
    #     corpus = corpus[1:10]
    #     print corpus[4]
    #     vectorizer = CountVectorizer(min_df=1)
    #     X = vectorizer.fit_transform(corpus)
    #
    # print type(X[4].todense())
    # print vectorizer.get_feature_names()

    dictionary = {}
    if corpus:
        for sentence in corpus:
            temp = sentence.split(' ')
            for word in temp:
                if word in dictionary:
                    dictionary[word] += 1
                else:
                    dictionary[word] = 1

    import operator

    sorted_x = sorted(dictionary.items(), key=operator.itemgetter(1))
    for i in sorted_x:
        print i[0]