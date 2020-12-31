# -*- coding: utf-8 -*-
"""
Created on Sun May 29 14:37:34 2019

@author: sk
"""

import pandas as pd
import csv, ast, psycopg2

def dataType(val, current_type):
    try:
        # Evaluates numbers to an appropriate type, and strings an error
        t = ast.literal_eval(val)
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    
    if type(t) in [int, float]:        
        if (type(t) in [int]) and current_type not in ['float', 'varchar']:
           # Use smallest possible int type
           if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
               return 'smallint'
           elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
               return 'int'
           else:
               return 'bigint'
        if type(t) is float and current_type not in ['varchar']:
              return 'decimal'
    else:
        return 'varchar'

f = open('E:/test2.csv','r')

reader = csv.reader(f)

longest, headers, type_list = [], [], []

for row in reader:
    if len(headers) == 0:
        headers = row
        for col in row:
            longest.append(0)
            type_list.append('')
        # print(longest)
        # print(type_list)
    else:
        for i in range(len(row)):
            # NA is the csv null value
            if type_list[i] == 'varchar' or row[i] == 'NA':
                pass
            else:
                var_type = dataType(row[i], type_list[i])
                type_list[i] = var_type
            if len(row[i]) > longest[i]:
                longest[i] = len(row[i]) + 10

#print(longest)
#print(headers)
#print(type_list)
f.close()

statement = 'create table test2Bal ('

for i in range(len(headers)):
    if type_list[i] == 'varchar':
        statement = (statement + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
    else:
        statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

statement = statement[:-1] + ');'

print(statement)

conn = psycopg2.connect(dbname= 'adventureworksdw', host='redshift.xx.us-east-1.redshift.amazonaws.com', 
                      port= '5439', user= 'awsuser', password= '')

cur = conn.cursor()

# cur.execute(statement)
# conn.commit()

sql = """copy test2Bal from 's3://inferschema2019/test2.csv'
    access_key_id ''
    secret_access_key ''
    region 'us-east-1'
    ignoreheader 1
    null as 'NA'
    removequotes
    delimiter ',';"""
cur.execute(sql)
conn.commit()
print("s3 file loaded to redshift !")

conn.close()




