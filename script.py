import os
import pymongo
import datetime
import time
import datetime
from bson.code import Code

symbol = {"ES" : {sessionStartTime: time.strptime("9:30:00","%H:%M:%S"), sessionEndTime: time.strptime("16:15:00","%H:%M:%S")}} 

def GetMap():
    map = Code("function () {"\
                 "    emit(this.p, this.v);"\
                 "}")
    return map
      
def GetReduce():
      reduce = Code("function(key, values) {"\
                      "var volume = 0;"\
                      "  for (var i = 0; i < values.length; i++) {"\
                      "    volume += values[i];"\
                      "  }"\
                      "  return volume;"\
                      "}")
      return reduce

def GetQuery(startTime,endTime):
     query = {"date": {"$gte": startTime, "$lt" : endTime}}
     return query
    
    
def ProcessFile():
    """Takes a file name and inserts the tick data into the database"""
    from time import mktime
    from datetime import datetime
    
    Myfile = open("./data.txt")
    print "first line %s" % (Myfile.readline())
    coll = GetConnection().es_ticks
    index = 0
    for line in Myfile:
        if index == 10000:
            break
        symbol, date, price, volume, bid, ask = line.split('\t')
        post = {'date': datetime.fromtimestamp(mktime(time.strptime(date,"%m/%d/%Y %H:%M:%S")))
                ,'bid': float(bid)
                ,'ask': float(ask)
                ,'p': float(price)
                ,'v': int(volume)}
        coll.insert(post)
        index += 1
    Myfile.close()
def GetConnection():
    from pymongo import Connection
    connection = Connection("mongodb://jermeyz:zeidner1@staff.mongohq.com:10031/ES_DATA")
    db = connection['ES_DATA']
    #collection = db.es_ticks
    return db

def CreateRangeForMinutes():
    from datetime import timedelta
    
    conn = GetConnection()
    coll = conn.es_ticks
    rollup = conn.rollup
    startTime = datetime.datetime(2011,4,4,9,30,00)
    endTime = datetime.datetime(2011,4,4,4,15,00)

    tempDate = datetime.datetime(2011,4,4,9,30,00)


    for i in range(1,3): # 15 half hour sessions and 1 15 minute
        startTimeDelta  = timedelta(minutes=30 * i)
        endTimeDelta = timedelta(minutes=30 * (i - 1))
        if i == 14: # last one is only 15 minutes
            temp = 30 * i
            startTimeDelta  = datetime.timedelta(minutes= temp - 15)
        else :
            startTimeDelta  = datetime.timedelta(minutes=30 * i)
        startTime = tempDate + endTimeDelta
        endTime = tempDate + startTimeDelta
        print "start: %s end: %s" % (startTime,endTime)
        result = coll.map_reduce(GetMap(),GetReduce(),"myresult",GetQuery(startTime,endTime))
        print result
        for m in result.find():
            print m
            #insert the volume at price for that given period
            doc = {'start_time': startTime,
                   'end_time': endTime,
                   'v' : m['value'],
                   'price': m['_id']}
            rollup.insert(doc)
        conn.drop_collection("myresult")


    # get all records in the range and get the volume at each price
if __name__ == '__main__' :
    CreateRangeForMinutes()
    #ProcessFile() 
