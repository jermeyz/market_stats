import os
import pymongo
import datetime
import time
import datetime
import math
from bson.code import Code

class Symbol:
    def __init__(self,ticker,startTime,endTime):
        self.ticker = ticker
        self.startTime = startTime
        self.endTime = endTime

es = {"symbol" : "ES" ,  "sessionStartTime" : datetime.time(hour=9,minute=30,second=00) ,
                          "sessionEndTime" : datetime.time(hour=16,minute=15,second=00) }

def GetMap():
    map = Code("function () {"\
                 "  var b = 0,a = 0;      "\
                 " if(this.p <= this.bid) {   "\
                 "      b = this.v;     }  "\
                 " if(this.p >= this.ask) {  "\
                 "      a = this.v;  }   "\
                 "    emit(this.p, {volume:this.v, ask: a, bid: b} );"\
                 "}")
    return map
      
def GetReduce():
      reduce = Code("function(key, values) {"\
                      "var volume = 0,bid = 0,ask = 0;"\
                      "  for (var i = 0; i < values.length; i++) {"\
                      "    volume += values[i].volume;"\
                      "    bid += values[i].bid;"\
                      "    ask += values[i].ask;"\
                      "  }"\
                      "  return {volume: volume,bid:bid,ask:ask};"\
                      "}")
      return reduce

def GetQuery(startTime,endTime):
     query = {"date": {"$gte": startTime, "$lte" : endTime}}
     return query
    
    
def ProcessFile(fileName,date,symbol):
    """Takes a file name and inserts the tick data into the database"""
    from time import mktime
    from datetime import datetime
    
    Myfile = open("./" + fileName)
    print "first line %s" % (Myfile.readline())
    conn = GetConnection()
    coll = conn[GetCollectionName(symbol,date)]
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
def GetRange(symbol,minutes):
    #get total length of session and divide by minutes
    #need a fake date to subtract
    start = datetime.datetime(1900,1,1,symbol["sessionStartTime"].hour,symbol["sessionStartTime"].minute,symbol["sessionStartTime"].second)
    end = datetime.datetime(1900,1,1,symbol["sessionEndTime"].hour,symbol["sessionEndTime"].minute,symbol["sessionEndTime"].second)
 
    diff =  end - start
    numOfUnitsOfMinutes =  float((diff.seconds /60 )) / float(minutes)
    x = math.ceil(numOfUnitsOfMinutes)
    return range(1,int(x) + 1)
def GetCollectionName(symbol, date):
    return symbol["symbol"]  + "-" + str(date.year) + "-" + str(date.month) + "-" + str(date.day)
def CreateStatsForRangeOfMinutes(symbol,minutesPerBar,date):
    from datetime import timedelta
    
    conn = GetConnection()
    coll = conn[GetCollectionName(symbol,date)]
    #rollup = conn.rollup
   

    startTime = date + timedelta(hours=symbol["sessionStartTime"].hour,minutes=symbol["sessionStartTime"].minute,seconds=symbol["sessionStartTime"].second )

    for i in GetRange(symbol,30): 
       
        rangeStartTime  = startTime + timedelta(minutes=minutesPerBar* (i-1))
        rangeEndTime = rangeStartTime + timedelta(minutes=minutesPerBar,seconds=-1)

        if rangeEndTime.time() > symbol["sessionEndTime"]:
            rangeEndTime = date + timedelta(hours=symbol["sessionEndTime"].hour,minutes=symbol["sessionEndTime"].minute,seconds=symbol["sessionEndTime"].second) 
  
        print "start: %s end: %s" % (rangeStartTime,rangeEndTime)

       
        result = coll.map_reduce(GetMap(),GetReduce(),"myresult",GetQuery(rangeStartTime,rangeEndTime))
        print result
        for m in result.find():
            print m
##            #insert the volume at price for that given period
##            doc = {'start_time': startTime,
##                   'end_time': endTime,
##                   'v' : m['value'],
##                   'price': m['_id']}
##            rollup.insert(doc)
##        conn.drop_collection("myresult")


    # get all records in the range and get the volume at each price
if __name__ == '__main__' :
    CreateStatsForRangeOfMinutes(es,30,datetime.datetime(2011,4,4))
    #ProcessFile("data.txt",datetime.datetime(2011,4,4),es)
    #print GetCollectionName(es,datetime.datetime(2011,12,12))
    #print GetRange(es,30)

