
'''
#!/usr/bin/python3
import psycopg2

conn = psycopg2.connect("host=localhost dbname=postgres user=ricedb password=zl15ricedb")

cur = conn.cursor()
# cur.execute("DROP TABLE IF EXISTS test;")
# cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
# cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
# cur.execute("SELECT * FROM test;")
# print(cur.fetchone())


#cur.callproc('CubeVolume', (5,))
#cur.execute("DROP TABLE IF EXISTS Org;")

#cur.execute("INSERT INTO Org (id, name, is_univ) VALUES (%s, %s, %s)", ("Org1", "Rice_U", True,))
#cur.execute("SELECT * FROM Org;")





#print(cur.fetchone())
rows = cur.fetchall()
for row in rows:
    print(row)

# Make the changes to the database persistent
conn.commit()

# Close communication with the database
cur.close()
conn.close()
'''

import csv

with open('hw6.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',')
    curTable = None
    for row in spamreader:
        if(row[0].startswith("*")):
            print(row[0].strip(',*'))
            curTable = row[0].strip(',*')
        else:
            if curTable=='Participant':
                length = 4
                newrow = row[:length]
                print(newrow)
                parameters = row[0].rstrip(',').split(',')
        # print('\n')
        # print(type(row))
        # print(', '.join(row))
