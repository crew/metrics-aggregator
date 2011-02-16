import sys
import json
import logging
from crew.metrics.httpapi import HttpAPI



def get_records_from_json(filename):
    f = open(filename, 'rb')
    for line in f.readlines():
        yield json.loads(line.strip())

class LinuxData(object):

    def __init__(self, ns, apikey, url):
        self.ns = ns
        self.apikey = apikey
        self.url = url
        self.api = HttpAPI(namespace=self.ns, apikey=self.apikey, url=self.url)

    def store(self, count, timestamp):
        #logging.debug('store: count=%s timestamp=%s', count, timestamp)
        resp = self.api.store(count=count,timestamp=float(timestamp))
        if not resp['code'] == 201:
            raise Exception('Endpoint error.')                       


def main(filename, ns, apikey, url):
    ld = LinuxData(ns, apikey, url)
    user_count={}
    user_count["tty"]=0
    user_count["pts"]=0
    usernames_tty={}
    usernames_pts={}
    last_seconds=None
    for record in get_records_from_json(filename):
        # --
        seconds=record.get("seconds")
        if (last_seconds == None):
            last_seconds=seconds
        elif (seconds - last_seconds >= 600):
            # -- 10 minutes passed --
            print "store: usercount: %d seconds %d" % user_count["tty"] , seconds
            ld.store(user_count["tty"],seconds)
        user=record.get("user")
        logintype=record.get("type")
        device=record.get("device")
        print "device: " , device
        if device.startswith("tty"):
            if (logintype == "USER_PROCESS"):
                # log in
                user_count["tty"]+=1
                print "log in tty: " , user_count["tty"]
                if user in usernames_tty:
                    usernames_tty[user]+=1
                else:
                    usernames_tty[user]=1
            elif (logintype == "DEAD_PROCESS"):
                # log out
                print "log out tty: " , user_count["tty"]
                user_count["tty"]-=1
                if user in usernames_tty:
                    if usernames_tty[user] > 0:
                        # we accounted for this user logging in
                        usernames_tty[user]-=1
                    else:
                        pass
                        # we can not account for this user logging in.
                        # ! dont decrease count below zero !
                        # TODO: increase old counts somehow?
                else:
                    pass
                    # we can not account for this user logging in.
                    # ! dont decrease count below zero !
                    # TODO: increase old counts somehow?
        elif device.startswith("pts"):
            if (logintype == "USER_PROCESS"):
                # log in
                user_count["pts"]+=1
                if user in usernames_pts:
                    usernames_pts[user]+=1
                else:
                    usernames_pts[user]=1
            elif (logintype == "DEAD_PROCESS"):
                # log out
                user_count["pts"]-=1
                if user in usernames_pts:
                    if usernames_pts[user] > 0:
                        # we accounted for this user logging in
                        usernames_pts[user]-=1
                    else:
                        pass
                        # we can not account for this user logging in.
                        # ! dont decrease count below zero !
                        # TODO: increase old counts somehow?
                else:
                    pass
                    # we can not account for this user logging in.
                    # ! dont decrease count below zero !
                    # TODO: increase old counts somehow?           

if __name__ == '__main__':
    #FLAGS(sys.argv)    
    #logging.basicConfig(filename=FLAGS.logfile, level=logging.DEBUG)
    print "hello"
    main(sys.argv[1],"linux","any string","http://pacman.ccs.neu.edu:2000")
#    except Exception as e:
 #       print e
  #      pass
        #logging.error(e)
