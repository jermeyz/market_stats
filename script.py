import os
import pymongo
import datetime
import time
import datetime


def ProcessFile():
    """Takes a file name and inserts the tick data into the database"""
    from time import mktime
    from datetime import datetime
    
    Myfile = open("./data.txt")
    print "first line %s" % (Myfile.readline())
    coll = ConnectToMongo()
    for line in Myfile:
        symbol, date, price, volume, bid, ask = line.split('\t')
        post = {'date': datetime.fromtimestamp(mktime(time.strptime(date,"%m/%d/%Y %H:%M:%S")))
                ,'bid': float(bid)
                ,'ask': float(ask)
                ,'p': float(price)
                ,'v': int(volume)}
        coll.insert(post)
    Myfile.close()
def ConnectToMongo():
    from pymongo import Connection
    connection = Connection("mongodb://jermeyz:zeidner1@flame.mongohq.com:27039/ES_DATA")
    db = connection['ES_DATA']
    collection = db.es_ticks
    return collection
def CreateRangeForMinutes(minutes):
    from datetime import timedelta
    from bson.code import Code

    coll = ConnectToMongo()
    startTime = datetime.datetime(2011,4,4,9,30,00)
    endTime = datetime.datetime(2011,4,4,4,15,00)

    tempDate = datetime.datetime(2011,4,4,9,30,00)
##    d = datetime(2011, 4, 4, 9,30,00)
##    s = datetime(2011,4,4,9,31,00)
    for i in range(1,15):
        startTimeDelta  = timedelta(minutes=30 * i)
        endTimeDelta = timedelta(minutes=30 * (i -1))
        if i == 14:
            temp = 30 * i
            startTimeDelta  = datetime.timedelta(minutes= temp - 15)
        else :
            startTimeDelta  = datetime.timedelta(minutes=30 * i)
        startTime = tempDate + endTimeDelta
        endTime = tempDate + startTimeDelta
        #print "start: %s end: %s" % (startTime,endTime)
        map = Code("function () {"\
                 "    emit(this.p, this.v);"\
                 "}")
        reduce = Code("")
        query = {"date": {"$gte": startTime, "$lt" : endTime}}
        print "records %s for %s to %s" %\
        (coll.find(query).count(),startTime,endTime)
##    for post in coll.find({"date": {"$gte": d, "$lt" : s}}):
##        print post
##    print "done"
    
    # get all records in the range and get the volume at each price
if __name__ == '__main__' :
    CreateRangeForMinutes(30)
    #ProcessFile() 
