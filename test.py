import os
import pymongo
import datetime
import time

def ProcessFile():
    """Takes a file name and inserts the tick data into the database"""
    from time import mktime
    from datetime import datetime
    
    Myfile = open("./data.txt")
    print "first line %s" % (Myfile.readline())
    coll = ConnectToMongo()
    for line in Myfile:
        symbol, date, price, volume, bid, ask = line.split('\t')
        post = {'date': datetime.fromtimestamp(mktime(time.strptime(date,"%m/%d/%Y %I:%M:%S"))) ,'bid': float(bid) , 'ask': float(ask) , 'p': float(price) , 'v': int(volume)}
        #print post['date']
        coll.insert(post)
    Myfile.close()
def ConnectToMongo():
    from pymongo import Connection
    connection = Connection("mongodb://jermeyz:zeidner1@flame.mongohq.com:27039/ES_DATA")
    db = connection['ES_DATA']
    collection = db.es_ticks
    return collection
    #post = {'jz' : 'test'}
    #collection.insert(post)
def CreateRangeForMinutes(minutes):
    from datetime import datetime
    coll = ConnectToMongo()
    for post in coll.find({ 'volume' : '5' } ):
        print post
    print "done"
if __name__ == '__main__' :
    #CreateRangeForMinutes(10)
    ProcessFile()
