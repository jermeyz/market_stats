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

es = Symbol("ES",datetime.time(hour=9,minute=30,second=00),datetime.time(hour=16,minute=15,second=00))

#def GetMapForDailyVolume():
    

def GetMap():
    map = Code("function () {"\
                 "  var b = 0,a = 0;      "\
                 " if(this.p <= this.bid) {   "\
                 "      b = this.v;     }  "\
                 " if(this.p >= this.ask) {  "\
                 "      a = this.v;  }   "\
                 "    emit(this.p, {volume:this.v, ask: a, bid: b,v:1} );"\
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
def GetQueryForTotalVolume(date,symbol):
    startDate = datetime.datetime(year=date.year,month=date.month,day=date.day,hour=symbol.startTime.hour,minute=symbol.startTime.minute,second=symbol.startTime.second)
    endDate = datetime.datetime(year=date.year,month=date.month,day=date.day,hour=symbol.endTime.hour,minute=symbol.endTime.minute,second=symbol.endTime.second)
    query = {"start_time" : {"$gte" : startDate} , "end_time" : {"$lte" : endDate}}
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
    return db
def GetRange(symbol,minutes):
    #get total length of session and divide by minutes
    #need a fake date to subtract

    start = datetime.datetime(1900,1,1,symbol.startTime.hour,symbol.startTime.minute,symbol.startTime.second)
    end = datetime.datetime(1900,1,1,symbol.endTime.hour,symbol.endTime.minute,symbol.endTime.second)
 
    diff =  end - start
    numOfUnitsOfMinutes =  float((diff.seconds /60 )) / float(minutes)
    x = math.ceil(numOfUnitsOfMinutes)
    return range(1,int(x) + 1)
def GetCollectionName(symbol, date):
    return symbol.ticker  + "-" + str(date.year) + "-" + str(date.month) + "-" + str(date.day)
def Get30MinuteBarCollection(symbol):
    return symbol.ticker+ "-30-minute-bars"
def GetStatsCollectionName(symbol):
    return symbol.ticker + "-STATS"
def DailyStats(Date,symbol):

    conn = GetConnection()
    
    queryCollection = conn[Get30MinuteBarCollection(symbol)]
    statsCollection = conn[GetStatsCollectionName(symbol)]
    
    result = queryCollection.find(GetQueryForTotalVolume(date,symbol))
    print sDateTime
    print eDateTime

    for i in result:
        print i
    
    #statsCollection.map_reduce(GetQueryForAverageVolume(startTime,endTime))
    
    statsCollection.insert({'total_volume_900_415_all' : { 'date' : 34 , 'value' : 4} })
def CreateStatsForRangeOfMinutes(symbol,minutesPerBar,date):
    from datetime import timedelta
    
    conn = GetConnection()
    coll = conn[GetCollectionName(symbol,date)]
    rollup = conn[Get30MinuteBarCollection(symbol)]
   

    startTime = date + timedelta(hours=symbol.startTime.hour,minutes=symbol.startTime.minute,seconds=symbol.startTime.second )

    for i in GetRange(symbol,30): 
       
        rangeStartTime  = startTime + timedelta(minutes=minutesPerBar* (i-1))
        rangeEndTime = rangeStartTime + timedelta(minutes=minutesPerBar,seconds=-1)

        if rangeEndTime.time() > symbol.endTime:
            rangeEndTime = date + timedelta(hours=symbol.endTime.hour,minutes=symbol.endTime.minute,seconds=symbol.endTime.second) 
  
        print "start: %s end: %s" % (rangeStartTime,rangeEndTime)
       
        result = coll.map_reduce(GetMap(),GetReduce(),"myresult",query=GetQuery(rangeStartTime,rangeEndTime))
        print result.count()
        for m in result.find():
            print m
            #insert the volume at price for that given period
            doc = {'start_time': rangeStartTime,
                   'end_time': rangeEndTime,
                   'volume' : m['value']['volume'],
                   'price': m['_id'],
                   'bid' : m['value']['bid'],
                   'ask' : m['value']['ask']}
            rollup.insert(doc)
        conn.drop_collection("myresult")

    ##create stats for this date
    ##stats that we want to create
    ## for each x minutesBar avg volume and range
    ## range and volume for the day
    #statsCollection = conn[GetStatsCollectionName(symbol)]
    #statsCollection.insert({'avg_volume_900_4:15_all' : { 'date' : 34 , 'value' : 4} })

    # get all records in the range and get the volume at each price
if __name__ == '__main__' :
    #CreateStatsForRangeOfMinutes(es,30,datetime.datetime(2011,1,1))
    #CreateStatsForRangeOfMinutes(es,30,datetime.datetime(2011,1,2))
    #ProcessFile("test_data-1-2-2011.txt",datetime.datetime(2011,1,2),es)
    #print GetCollectionName(es,datetime.datetime(2011,12,12))
    #print GetRange(es,30)
    DailyStats(datetime.datetime(2011,1,1),es)

