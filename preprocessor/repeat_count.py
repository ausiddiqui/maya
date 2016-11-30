# -*- coding: utf-8 -*-
from database import Database

database = Database(
    'localhost',
    'ds_test',
    'root',
    'root',
    'utf8mb4'
)

connection = database.connect_with_pymysql()
try:
    with connection.cursor() as cursor:
        # sql = "SELECT id,body,source,created_at FROM questions where status='spam'"
        # cursor.execute(sql)
        # data = cursor.fetchall()
        #
        # for record in data:
        #     sql = "SELECT id,created_at FROM questions WHERE id!=" + str(record['id']) + " and body='" + record['body'] + "'"
        #     cursor.execute(sql)
        #     data = cursor.fetchone()
        #     if data:
        #         sql = "INSERT INTO spam(spam_id,body,is_repeat,repeat_from,source,created_at,source_created_at) " \
        #               "VALUES('"+str(record['id'])+"','"+record['body']+"','1','"+str(data['id'])+"','"+record['source']+"','"+str(record['created_at'])+"','"+str(data['created_at'])+"')"
        #         cursor.execute(sql)
        #         connection.commit()
        #     else:
        #         sql = "INSERT INTO spam(spam_id,body,is_repeat,source,created_at) " \
        #               "VALUES('" + str(record['id']) + "','" + record['body'] +"','0','" + record['source'] + "','" + str(record['created_at']) + "')"
        #         cursor.execute(sql)
        #         connection.commit()

        # greetings = ['hi ','hello ','bye ']
        #
        # sql = "SELECT id,body FROM spam where is_repeat=0"
        # cursor.execute(sql)
        # data = cursor.fetchall()
        # for record in data:
        #     for i in greetings:
        #         if i in record['body']:
        #             print str(record['id']) + ' ' + record['body']

        # sql = "SELECT id,created_at,source_created_at FROM spam where is_repeat=1 and created_at>source_created_at"
        # cursor.execute(sql)
        # data = cursor.fetchall()
        #
        # timeline = []
        # for record in data:
        #     i = abs((record['created_at'] - record['source_created_at']).total_seconds()/60)
        #     timeline.append(i)
        #
        # import matplotlib.pyplot as plt
        # import pandas as pd
        # import numpy as np
        # import sys
        # plt.style.use('ggplot')
        # ranges = [0, .1, .25, .5, 1, 2, 3, 4, 5, 10, 30, 60, 120, 300, 600, 1440, 10080, sys.maxint]
        # col = ['<6s', '6-15sec', '15-30sec', '30sec-1min', '1-2min', '2-3min', '3-4min', '4-5min', '5-10min',
        #        '10-30min', '30-60min', '1-2hr', '2-5hr', '5-10hr', '10hr-1day', '1-7day', '>1week']
        # val = np.zeros(17)
        #
        # for i in range(len(ranges)-1):
        #     for j in timeline:
        #         if ranges[i] <= j < ranges[i+1]:
        #             val[i] += 1
        # df2 = pd.DataFrame(np.array(val), col, columns=['Count of repeated questions by time'])
        # df2.plot.bar()
        # plt.show()

        # sql = "SELECT id,body from spam where length(body)<20"
        # cursor.execute(sql)
        # data = cursor.fetchall()
        # count = 0
        # for record in data:
        #     if 'test' in record['body'].lower():
        #         print record['body']
        #         count += 1
        #     elif 'check' in record['body'].lower():
        #         print record['body']
        #         count += 1
        #
        # print count

        sql = "SELECT id,body from spam where length(body)<20"
        cursor.execute(sql)
        data = cursor.fetchall()
        count = 0
        mean = 0
        test = 0
        for record in data:
            if 'hi' in record['body'].lower():
                #print record['body']
                count += 1
            elif 'hello' in record['body'].lower():
                #print record['body']
                count += 1
            elif 'কেমন'.decode('utf-8') in record['body']:
                #print record['body']
                count += 1
            elif 'bye' in record['body'].lower():
                #print record['body']
                count += 1
            elif 'thank' in record['body'].lower():
                #print record['body']
                count += 1
            elif 'check' in record['body'].lower():
                #print record['body']
                test += 1
            elif 'test' in record['body'].lower():
                #print record['body']
                test += 1
            else:
                print record['body']
                mean += 1


        print mean

finally:
    # #closing the connection
    connection.close()
