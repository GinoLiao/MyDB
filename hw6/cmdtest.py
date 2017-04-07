import cmd
import psycopg2


class SwimMeetDBApp(cmd.Cmd):
    """Simple command processor example."""
    intro = 'Welcome to the Swim Meeting DB shell.  Type help or ? to list commands.\nConnect to your DB by using command:\nconnect [hostname] [dbname] [username] [password]'
    prompt = '(swim) '
    file = None

    conn = None
    cur = None
    #connect to db
    def do_connect(self, args):
        l = args.split()
        if len(l)!=4:
            print("*** invalid number of arguments.\nconnect [hostname] [dbname] [username] [password]")
            return
        try:
            addr = "host=" + l[0] + " dbname=" + l[1] + " user=" + l[2] + " password=" + l[3]
            conn = psycopg2.connect(addr)
            cur = conn.cursor()
            print("Success!")
        except:
            print("Connection failed, check your inputs")				

    def help_connect(self):
        print ('\n'.join([ 'connect [hostname] [dbname] [username] [password]',
                           'Connect to the database',
                           ]) )

    def do_q(self):
        # Make the changes to the database persistent
        conn.commit()

        # Close communication with the database
        cur.close()
        conn.close()
    #read csv


    #save to csv


    #Modify insert


    #modify update



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
    
    def do_EOF(self, line):
        return True

if __name__ == '__main__':
    SwimMeetDBApp().cmdloop()