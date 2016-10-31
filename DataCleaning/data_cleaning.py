# -*- coding: utf-8 -*-
"""
Script for Importing data from MySQL database and cleaning

"""
import os
import pymysql
import pandas as pd
from bs4 import BeautifulSoup

## Getting Data
# Changing directory
os.chdir("")

# Running the file containing MySQL information
execfile("connection_config.py")

# Connecting to MySQL
connection = pymysql.connect(host=hostname, user=usr, password=pwd, db=db, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# Fetching data from the database
with connection.cursor() as cursor:
    sql = "SELECT * FROM answers" 
    cursor.execute(sql)
    result = cursor.fetchall()

# Closing connection
connection.close()

# Saving the data as a dataframe
data = pd.DataFrame(result)

# Saving the data to an excel file
#data.to_excel("answers.xlsx")

# Importing data from the excel file
#data = pd.read_excel("answers.xlsx")


## Data cleaning
data["body"] = data["body"].fillna("")

# Cleaning html
nrow = data.shape[0]
body = list()

for i in range(0, nrow):
    body.append(BeautifulSoup(data["body"][i], "html"))
    body[i] = body[i].get_text()

body = pd.Series(body)

# Cleaning escape characters
body_new = body.str.replace("[\r\n\t]", "")
body_new = body_new.str.replace("[\\\\{2}]", " ")

# Fixing unicode


data["body"] = body_new
