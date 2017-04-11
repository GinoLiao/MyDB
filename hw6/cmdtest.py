import cmd
import psycopg2
import shlex
import datetime
import csv
import sys

class SwimMeetDBApp(cmd.Cmd):
    """Swim Meeting Database application."""
    intro = 'Welcome to the Swim Meeting DB shell.  Type help or ? to list commands.\nConnect to your DB by using command:\nconnect [hostname] [dbname] [username] [password]'
    prompt = '(swim) '
    file = None
    #length of parameters in table
    lendict={'Org':3, 'Meet':4, 'Participant':4, 'Stroke':1,
            'Distance':1, 'Leg':1, 'Event':3, 'StrokeOf':3,
            'Heat':3, 'Swim':6}
    #length of primary keys in table
    pk_lendict={'Org':1, 'Meet':1, 'Participant':1, 'Stroke':1,
            'Distance':1, 'Leg':1, 'Event':1, 'StrokeOf':2,
            'Heat':3, 'Swim':4}
    #tables names in insert order
    #this ensures we write CSV file in correct order so that when user 
    #read the written CSV file, he or she can read and insert tables to
    #DB in the default order of written CSV file
    table_names = ['Org', 'Meet', 'Participant', 'Leg',
                 'Stroke', 'Distance', 'Event', 'StrokeOf',
                 'Heat', 'Swim']
    #params = None   #connection parameters
    params = "host=localhost dbname=postgres user=ricedb password=zl15ricedb"
    
    
    #connect to db
    def do_connect(self, args):
        print(args)
        print(type(args))
        
        l = shlex.split(args)
        print(l[0])
        print(l[1])
        if len(l)!=4:
            print("*** invalid number of arguments.\nconnect [hostname] [dbname] [username] [password]")
            return
        try:
            #self.params = "host=" + l[0] + " dbname=" + l[1] + " user=" + l[2] + " password=" + l[3]
            conn = psycopg2.connect(self.params)
            cur = conn.cursor()
            conn.commit()
            cur.close()
            conn.close()
            print("Success!")
        except:
            print("Connection failed, check your inputs")				

    def help_connect(self):
        print ('\n'.join([ 'connect [hostname] [dbname] [username] [password]',
                           'Connect to the database',
                           ]) )
    
    #########################################
    #########################################
    #############Upsert Functions############
    #########################################
    #########################################

    #update or insert an organization or university
    def do_upsertOrg(self, args):
        l = shlex.split(args)
        if len(l)!=3:
            print("*** invalid number of arguments.")
            return
        #id input should not be null or empty
        if l[0]=='' or l[0]=='NULL':
            print('The [id] field of upsertOrg command cannot be empty or NULL.')
            return
        id = l[0]
        name = l[1]
        #is_univ input should not be null or empty
        if l[2]=='' or l[2]=='NULL':
            print('The [is_univ] field of upsertOrg command cannot be empty or NULL.')
            return
        #is_univ should be either true or false
        if l[2].lower() not in ['true', 'false']:
            print('Please enter "true" or "false" for [is_univ] field')
            return
        is_univ = l[2]
        #self.upsert function will convert empty string or 'NULL'
        #in the [name] field to None
        #connect and upsert
        self.upsert('Org', l)


    def help_upsertOrg(self):
        print ('\n'.join([ '',
                           'upsertOrg [org_id] [name] [is_univ]',
                           'Update or insert a new organization',
                           'Primary key (org_id)',
                           '[org_id]: id of the organization or university',
                           '[name]: name of the organization or university',
                           '      If the name includes space, using quotation marks',
                           '      i.e. upsertOrg O001 "Rice University" true',
                           '[is_univ]: TRUE for university, FALSE for organization',
                           '         this field is case-insensitive',
                           ]) )

    #update or insert a Meet info
    def do_upsertMeet(self, args):
        l = shlex.split(args)
        if len(l)!=4:
            print("*** invalid number of arguments.")
            return
        #Leg input should not be null or empty
        if l[0]=='' or l[0]=='NULL':
            print('The [name] field of upsertMeet command cannot be empty or NULL.')
            return

        conn = None
        name = l[0]
        #check for valid date input
        if l[1] not in ['', 'NULL']:  
            try:
                datetime.datetime.strptime(l[1], '%Y-%m-%d')
            except ValueError:
                print("Incorrect data format, should be YYYY-MM-DD")
                return
        start_date = l[1]

        #check for valid num_days
        if l[2] not in ['', 'NULL']:      
            try:
                a = int(l[2])
                if a <= 0:
                    print("[num_days] should be an integer larger than 0")
                    return
            except ValueError:
                print("Incorrect data format, [num_days] should be an integer")
                return
        num_days = l[2]
        org_id = l[3]

        #self.upsert function will convert empty string or 'NULL'
        #in the last 3 fields to None
        #connect and upsert
        self.upsert('Meet', l)
                

    def help_upsertMeet(self):
        print ('\n'.join([ '',
                           'upsertMeet [org_id] [name] [is_univ]',
                           'Update or insert a new Meet info',
                           'Primary key (name)',
                           '[name]: name of the Swim Meeting',
                           '      If the name includes space, using quotation marks',
                           '      i.e. upsertMeet xxx "Rice University" xxx xxx',
                           '[start_date]: YYYY-MM-DD',
                           '              start date of the Swim Meeting',
                           '[num_days]: number of days of this Swim Meeting',
                           '            Please enter a number larger than 0',
                           '[org_id]: id of the organization or university',
                           ]) )


    #Update or insert a participant info
    def do_upsertParticipant(self, args):
        l = shlex.split(args)
        if len(l)!=4:
            print("*** invalid number of arguments.")
            return

        #First 3 fields should not be null or empty
        if '' in l[:3] or 'NULL' in l[:3]:
            print('First 3 fields of upsertParticipant command cannot be empty or NULL.')
            return
        conn = None
        id = l[0]
        gender = l[1]
        #check for valid date input
        if gender not in ['M', 'F']:
            print("[gender] should be either 'M' or 'F'. ")
            return 
     
        org_id = l[2]

        #self.upsert function will convert empty string or 'NULL'in [name] field to None
        name = l[3]

        #connect and upsert
        self.upsert('Participant', l)
        
                

    def help_upsertParticipant(self):
        print ('\n'.join([ '',
                           'upsertParticipant [id] [gender] [org_id] [name]',
                           'Update or insert a participant info',
                           'Primary key (id)',
                           '[id]: id of the participant',
                           '[gender]: gender of the swimming',
                           '          [gender] should be either "M" or "F".',
                           '[org_id]: id of the university of this participant',
                           '[name]: name of this participant',
                           '      If the name includes space, using quotation marks',
                           '      i.e. upsertParticipant xxx xxx xxx "first_name last_name"',
                           ]) )



    #Update or insert a participant info
    def do_upsertLeg(self, args):
        l = shlex.split(args)
        if len(l)!=1:
            print("*** invalid number of arguments.")
            return

        #Leg input should not be null or empty
        if args=='' or args=='NULL':
            print('The [leg] field of upsertLeg command cannot be empty or NULL.')
            return

        conn = None
        leg = args
        #leg should be an integer larger than 0
        try:
            a = int(leg)
            if a <= 0:
                print("[leg] should be an integer larger than 0")
                return
        except ValueError:
            print("Incorrect data format, [leg] should be an integer")
            return

        #connect and upsert
        self.upsert('Leg', l)
                

    def help_upsertLeg(self):
        print ('\n'.join([ '',
                           'upsertLeg [leg]',
                           'Update or insert a Leg',
                           'Primary key (leg)',
                           '[leg]: a possible number of the legs in relay races',
                           '       [leg] should be an integer larger than 0.',
                           ]) )


    #Update or insert a participant info
    def do_upsertStroke(self, args):
        l = shlex.split(args)
        if len(l)!=1:
            print("*** invalid number of arguments.")
            return
        #stroke input should not be null or empty
        if args=='' or args=='NULL':
            print('The [stroke] field of upsertStroke command cannot be empty or NULL.')
            return
        conn = None
        stroke = args
        #connect and upsert
        self.upsert('Stroke', l)
                

    def help_upsertStroke(self):
        print ('\n'.join([ '',
                           'upsertStroke [stroke]',
                           'Update or insert a Stroke',
                           'Primary key (stroke)',
                           '[stroke]: a possible stroke in Swim Meeting',
                           ]) )



    #Update or insert a participant info
    def do_upsertDistance(self, args):
        l = shlex.split(args)
        if len(l)!=1:
            print("*** invalid number of arguments.")
            return
        
        #distance input should not be null or empty
        if args=='' or args=='NULL':
            print('The [distance] field of upsertDistance command cannot be empty or NULL.')
            return

        conn = None
        distance = args
        #distance should be an integer larger than 0
        try:
            a = int(distance)
            if a <= 0:
                print("[distance] should be an integer larger than 0")
                return
        except ValueError:
            print("Incorrect data format, [distance] should be an integer")
            return

        #connect and upsert
        self.upsert('Distance', l)
                

    def help_upsertDistance(self):
        print ('\n'.join([ '',
                           'upsertLeg [distance]',
                           'Update or insert a distance',
                           'Primary key (distance)',
                           '[distance]: a possible distance of races in the Swim Meeting',
                           '            [distance] should be an integer larger than 0.',
                           ]) )



    #Update or insert a participant info
    def do_upsertEvent(self, args):
        l = shlex.split(args)
        if len(l)!=3:
            print("*** invalid number of arguments.")
            return
        #all 3 fields should not be null or empty
        if '' in l or 'NULL' in l:
            print('No fields of upsertEvent command can be empty or NULL.')
            return
        conn = None
        id = l[0]
        gender = l[1]
        #gender should not be null
        if gender not in ['M', 'F']:
            print("[gender] should be either 'M' or 'F'.")
            return 
     
        distance = l[2]
        #distance should be an integer larger than 0
        try:
            a = int(distance)
            if a <= 0:
                print("[distance] should be an integer larger than 0")
                return
        except ValueError:
            print("Incorrect data format, [distance] should be an integer")
            return

        #connect and upsert
        self.upsert('Event', l)      


    def help_upsertEvent(self):
        print ('\n'.join([ '',
                           'upsertEvent [id] [gender] [distance]',
                           'Update or insert an Event info',
                           'Primary key (id)',
                           '[id]: id of the Event',
                           '[gender]: gender of the Event',
                           '          [gender] should be either "M" or "F".',
                           '[distance]: distance of this event. SHOULD NOT BE NULL',
                           '            [distance] should be an integer larger than 0.',
                           ]) )




    #Update or insert a participant info
    def do_upsertStrokeOf(self, args):
        l = shlex.split(args)
        if len(l)!=3:
            print("*** invalid number of arguments.")
            return
        #all 3 fields should not be null or empty
        if '' in l or 'NULL' in l:
            print('No fields of upsertStrokeOf command can be empty or NULL.')
            return
        conn = None
        event_id = l[0]
        leg = l[1]
        #leg should be an integer larger than 0
        try:
            a = int(leg)
            if a <= 0:
                print("[leg] should be an integer larger than 0")
                return
        except ValueError:
            print("Incorrect data format, [leg] should be an integer")
            return

        #stroke should not be null    
        stroke = l[2]

        #connect and upsert
        self.upsert('StrokeOf', l)


    def help_upsertStrokeOf(self):
        print ('\n'.join([ '',
                           'upsertEvent [event_id] [leg] [stroke]',
                           'Update or insert stroke of a leg of a particular event',
                           'Primary key (event_id, leg)',
                           '[event_id]: id of the Event',
                           '[leg]: a possible number of the legs in relay races',
                           '       [leg] should be an integer larger than 0.',
                           '[stroke]: stroke of this leg of this event. SHOULD NOT BE NULL',
                           ]) )




    #Update or insert a participant info
    def do_upsertHeat(self, args):
        l = shlex.split(args)
        if len(l)!=3:
            print("*** invalid number of arguments.")
            return
        conn = None
        #all 3 fields should not be null or empty
        if '' in l or 'NULL' in l:
            print('No fields of upsertHeat command can be empty or NULL.')
            return
        id = l[0]
        event_id = l[1] 
        meet_name = l[2]

        #connect and upsert
        self.upsert('Heat', l)

                

    def help_upsertHeat(self):
        print ('\n'.join([ '',
                           'upsertHeat [id] [event_id] [meet_name] ',
                           'Update or insert a particular heat.',
                           'Heat is a weak entity.',
                           'Primary key (id, event_id, meet_name)',
                           '[id]: weak id of a heat',
                           '[event_id]: id of the Event',
                           '[meet_name]: name of the Swim Meeting',
                           ]) )



    '''
    Update or insert a participant info
    '''
    def do_upsertSwim(self, args):
        l = shlex.split(args)
        if len(l)!=6:
            print("*** invalid number of arguments.")
            return
        conn = None
        #First 5 fields should not be null or empty
        if '' in l[:5] or 'NULL' in l[:5]:
            print('First 5 fields of upsertSwim command cannot be empty or NULL.')
            return

        heat_id = l[0]
        event_id = l[1] 
        meet_name = l[2]
        participant_id = l[3]
        leg = l[4]
        #[leg] should be an integer larger than 0
        try:
            a = int(leg)
            if a <= 0:
                print("[leg] should be an integer larger than 0")
                return
        except ValueError:
            print("Incorrect data format, [leg] should be an integer")
            return

        #if [time] is not empty or NULL, time should be a number larger than 0
        if l[5] != '' and l[5] != 'NULL':
            try:
                t = float(l[5])    #convert to float number
                if t <= 0:
                    print("[time] should be a number larger than 0")
                    return
            except ValueError:
                print("Incorrect data format, [time] should be a number")
                return
            l[5] = float(l[5])
        else:   #if [time] is empty or NULL, set it to None
            l[5] = None
        self.upsert('Swim', l)              

    def help_upsertSwim(self):
        print ('\n'.join([ '',
                           'upsertSwim [heat_id] [event_id] [meet_name] [participant_id] [leg] [t]',
                           'Update or insert result of a participant in a particular heat.',
                           'Primary key (heat_id, event_id, meet_name, participant_id)',
                           '[heat_id]: heat id of a particular event in a particular Swim Meeting',
                           '[event_id]: id of the Event',
                           '[meet_name]: name of the Swim Meeting',
                           '[participant_id]: id of the participant',
                           '[leg]: the leg that this swimmer participates. SHOULD NOT BE NULL',
                           '[time]: the finish time of this participant',
                           ]) )


    '''update or insert data in the row to the table.'''
    def upsert(self, table, row):
        conn = None
        function_name = 'Upsert' + table
        length = self.lendict[table]
        # if table != 'Swim':
        #     return

        #convert empty string or 'NULL' to None
        for i in range(length):
            if row[i]=='' or row[i]=='NULL':
                row[i] = None

        #type cast for float value in 'Swim'
        if table == 'Swim' and row[5] != None:
            row[5] = float(row[5])
        #print(row[:length])
        try:
            conn = psycopg2.connect(self.params)
            cur = conn.cursor()
            cur.callproc(function_name, row[:length])
            conn.commit()
            # close the communication with the PostgreSQL database server
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()



    #########################################
    #########################################
    #############Get Info Functions##########
    #########################################
    #########################################

    '''
    get info of an organization or university
    '''
    def do_get(self, args):
        l = shlex.split(args)
        #No fields can be null or empty
        if '' in l or 'NULL' in l:
            print('No fields in get command can be empty or NULL.')
            return

        if len(l) < 2:
            print("Please input parameters.")
            return

        table = l[0]
        if table not in self.pk_lendict.keys():
            print("Please enter a valid table name.")
            return

        if len(l) != self.pk_lendict[table] + 1:
            print("*** invalid number of arguments.")
            return

        conn = None
        function_name = 'Get' + table
        try:
            conn = psycopg2.connect(self.params)
            cur = conn.cursor()
            cur.callproc(function_name, l[1:])
            #print result
            rows = cur.fetchall()
            for row in rows:
                print(row)
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def help_get(self):
        print ('\n'.join([ '',
                           'getOrg [table] [param1] [param2] ...',
                           'Call a function to get information of the table',
                           '[table]: name of the table',
                           '         choices includes:',
                           '         Org, Meet, Participant, Stroke, Leg,',
                           '         Distance, Event, StrokeOf, Heat, Swim',
                           ]) )


    #########################################
    #########################################
    #############Read CSV Functions##########
    #########################################
    #########################################

    '''
    read csv, assuming the data file is valid. No error checking
    '''
    def do_readCSV(self, args):
        l = shlex.split(args)
        if len(l)!=1:
            print("*** invalid number of arguments. Should be 1.")
            return
        path = args
        try:
            with open(path, newline='') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=',')
                curTable = None
                count=0
                for row in spamreader:
                    if(row[0].startswith("*")):
                        print("count: ", count)
                        count=0
                        print(row[0].strip(',*'))
                        curTable = row[0].strip(',*')
                        
                    else:
                        #length = 4
                        #newrow = row[:length]
                        #print(newrow)
                        #print(parameters)
                        count+=1
                        self.upsert(curTable, row)
                print("count: ", count)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print("Please check your input path.")


# readCSV hw6.csv
    def help_readCSV(self):
        print ('\n'.join([ '',
                           'readCSV [path]',
                           'Read CSV file and insert or update data to database.',
                           '[path]: path of the target CSV file',
                           ]) )



    #########################################
    #########################################
    ##########Save to CSV Functions##########
    #########################################
    #########################################
    '''
    writeCSV save.csv
    save all data to user-provided csv file
    '''
    def do_writeCSV(self, args):
        l = shlex.split(args)
        if len(l)!=1:
            print("*** invalid number of arguments. Should be 1.")
            return
        #Limit the user to save data to CSV file
        path = args
        if not path.endswith('.csv'):
            print("The input path should point to a CSV file. ")
            return

        conn = None
        db_tables = {}
        #fetch table names and all rows in each table to dict
        for table in self.table_names:
            rows = self.callDBFunc('GetAll'+table, [])
            db_tables[table] = rows

        #write DB to file
        try:
            with open(path, 'w', newline='') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=',')
                #this ensures we write CSV file in correct order so that when user 
                #read the written CSV file, he or she can read and insert tables to
                #DB in the default order of written CSV file
                for table in self.table_names:
                    table_name = '*' + table
                    spamwriter.writerow([table_name]) 
                    for row in db_tables[table]:
                        spamwriter.writerow(row)    
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print("Please check your input path.")

# readCSV hw6.csv
    def help_writeCSV(self):
        print ('\n'.join([ '',
                           'writeCSV [path]',
                           'Write all data in database to user-provided CSV file.',
                           '[path]: path of the target CSV file',
                           ]) )


    #print results from query
    def printQuery(self, rows):
        if rows != None:
            for row in rows:
                print(row)

    '''
    call a function in database
    params--parameters of the function
    '''
    def callDBFunc(self, function_name, params):
        conn = None
        res = None
        try:
            conn = psycopg2.connect(self.params)
            cur = conn.cursor()
            cur.callproc(function_name, params)
            #save result
            res = cur.fetchall()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        return res


    #display heat sheet 
    #meet info

    #meet scores

    #meet school heats

    #meet school swimmers

    #meet participant

    #meet event 
    


    
    def do_greet(self, person):
        if person:
            print ("hi,", person)
        else:
            print ('hi')
    
    def help_greet(self):
        print ('\n'.join([ 'greet [person]',
                           'Greet the named person',
                           ]) )

    def help_app(self):
        print ('\n'.join([ 'NULL or "" will be treated as NULL in table',
                           'Use "" to input an empty field',
                           'i.e. upsertXXX xxx "" ',
                           'Use NULL to input NULL',
                           'i.e. upsertXXX xxx NULL',
                           ]) )
    
    def do_EOF(self, line):
        return True

if __name__ == '__main__':
    SwimMeetDBApp().cmdloop()