#!/usr/bin/env python

import psycopg2
import csv

def Read():
	print ("Read")

def Save():
	print ("Save")

def Modify():
	print ("Modify")

def Display():
	print ("Display")

if __name__ == "__main__":
	print ("Welcome to the database user system!\n")
	while True:
		print ("What do you want?\n\
1.Load a file into the database\n\
2.Save the data into a file\n\
3.Modify the data in the database\n\
4.Display a heat sheet\n\
(Please enter the number of your choice or enter q to exit)")
		choice = input ()
		if choice == "1":
			Read ()
		elif choice == "2":
			Save ()
		elif choice == "3":
			Modify ()
		elif choice == "4":
			Display ()
		elif choice == "q":
			break
		else:
			print ("Input error! Please try again...")
