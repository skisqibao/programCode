#!/usr/bin/python

import sqlite3

conn = sqlite3.connect('test.db')

print ("数据库打开成功")

c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS COMPANY
     (ID INTEGER PRIMARY KEY  AUTOINCREMENT     NOT NULL,
       NAME           TEXT    NOT NULL,
       AGE            INT     NOT NULL,
       ADDRESS        CHAR(50),
       SALARY         REAL);''')
print ("数据表创建成功")



c.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (11, 'Paul', 32, 'California', 20000.00 )")

c.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (12, 'Allen', 25, 'Texas', 15000.00 )")

c.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (13, 'Teddy', 23, 'Norway', 20000.00 )")

c.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
      VALUES (14, 'Mark', 25, 'Rich-Mond ', 65000.00 )")

print(c)
cursor = c.execute("SELECT id, name, address, salary  from COMPANY ")
print(len(cursor.fetchall()))
#c.execute("INSERT INTO COMPANY (NAME,AGE,ADDRESS,SALARY) \
#      VALUES ('Mark2', 15, 'Sich-Mond ', 62000.00 )")

c.execute('''CREATE TABLE IF NOT EXISTS diagnostics(
       id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
       uuid TEXT NOT NULL,
       device_sn TEXT NOT NULL,
       abnormal_sec  TEXT NOT NULL
       );
    ''')


cursor = c.execute("SELECT id, name, address, salary  from COMPANY where id = ?", (11))
print(cursor.fetchone())

conn.commit()
conn.close()
