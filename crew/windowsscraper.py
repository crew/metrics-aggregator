import sys
sys.path.insert(0, '..')
import time
import threading
from crew.metrics.httpapi import HttpAPI

#initial format: Hostname, Time, Username, OS Version,/n etc...

'''
actual format:
date hostname, domain-name 
date = month (3 letters) day time (as 24 hour)
domain-name= "CCIS-WINDOWS" | "NUNET"
hostname = a string (pokemon name)
'''


def f():
    try:
        f = open('test.txt', 'r')
        file_string = f.read()
        user_list = file_string.split("/n")
        user_list = [elem.strip() for elem in user_list]
    
        for user in user_list:
            temp_list = file_string.split(",")
            separated_user = [elem.strip() for elem in temp_list]

            mapping = {
                      "login":separated_user.pop(2),
                      "user-sessions":1,
                      "hostname":separated_user.pop(0),
                      "os":"Windows",
                      "version":separated_user.pop(1),
                      "timestamp":separated_user.pop(0)
                      #this is being given as a datetime, yes?
                      }

            api = HttpAPI(namespace='ns', apikey='apikey', hostname='localhost',
            port=2000, timeout=20)
  
            print mapping #for testing
    
            api.store(timestamp=time.time(), **mapping)

        f.close()

    except IOError:
        print "The file could not be accessed"


def main():
    ts = []
    for _ in range(1):
        t = threading.Thread(target=f)
        t.daemon = True
        t.start()
        ts.append(t)
    for t in ts:
        t.join()


if __name__ == '__main__':
    main()        
