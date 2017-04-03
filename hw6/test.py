#!/usr/bin/python3
import psycopg2

conn = psycopg2.connect("host=localhost dbname=postgres user=ricedb password=zl15ricedb")

cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS test;")
cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")


cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))

cur.execute("SELECT * FROM test;")

print(cur.fetchone())

conn.commit()
cur.close()
conn.close()


# def checkName(name):
#   checkName = input("Is your name " + name + "? ") 
  
#   if checkName.lower() == "yes":    
#     print("Hello,", name)  
#   else:    
#     name = input("We're sorry about that. What is your name again? ")    
#     print("Welcome,", name)

# checkName("Keenan")