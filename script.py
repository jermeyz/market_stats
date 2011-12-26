import os
import pymongo
import sys
import datetime
import time
import datetime
import math
import collections
import logging
from pprint import pprint 
from bson.code import Code
from datetime import timedelta
from collections import namedtuple

class Symbol:
   
    def __init__(self,ticker,startTime,endTime):
        self.ticker = ticker
        self.startTime = startTime
        self.endTime = endTime
    def GetSessionTimeSpan(self):
        return self.startTime.strftime("%H:%M:%S") + "-" + self.endTime.strftime("%H:%M:%S")
    def GetRange(self,minutes):
        #get total length of session and divide by minutes
        #need a fake date to subtract
        symbol = self
        start = datetime.datetime(1900,1,1,symbol.startTime.hour,symbol.startTime.minute,symbol.startTime.second)
        end = datetime.datetime(1900,1,1,symbol.endTime.hour,symbol.endTime.minute,symbol.endTime.second)
     
        diff =  end - start
        numOfUnitsOfMinutes =  float((diff.seconds /60 )) / float(minutes)
        x = math.ceil(numOfUnitsOfMinutes)
        return range(1,int(x) + 1)
    def GetSessionRange(self,minutesRange,date):
        
        symbol = self
        startTime = date + timedelta(hours=symbol.startTime.hour,minutes=symbol.startTime.minute,seconds=symbol.startTime.second )

        for i in self.GetRange(minutesRange):
            
            rangeStartTime  = startTime + timedelta(minutes=minutesRange * (i-1))
            rangeEndTime = rangeStartTime + timedelta(minutes=minutesRange,seconds=-1)
            #print "start: %s end: %s" % (rangeStartTime,rangeEndTime)
            if rangeEndTime.time() > symbol.endTime:
                rangeEndTime = date + timedelta(hours=symbol.endTime.hour,minutes=symbol.endTime.minute,seconds=symbol.endTime.second) 
            timeRange =  namedtuple('timeRange', 'rangeStartTime rangeEndTime')
            startEnd = timeRange(rangeStartTime,rangeEndTime)
            #yield (rangeStartTime,rangeEndTime)
            yield(startEnd)
    


    
def PrintQueryResult(query,collToQuery):
    conn = GetConnection()
    for item in conn[collToQuery].find(query):
        print pprint(item)
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
def GetMapForTotalVolume():
    map = Code("function () {"\
                " emit('1',{volume:this.volume} );"\
                "}")
    return map
def GetReduceForTotalVolume():
    reduce = Code("function(key, values) {"\
                        "var volume = 0;"\
                        " for( var i = 0; i < values.length; i++) {"\
                        " volume += values[i].volume;"\
                        "}"\
                        " return {volume: volume}"\
                        "}")
    return reduce
def GetMapForRange():
    map = Code("function () {"\
                 "    emit('1', {price:this.price} );"\
                 "}")
    return map
def GetReduceForRange():
    reduce = Code("function(key, values) {"\
                      "var high = 0, low = 1000000, range = 0 ;"\
                      "  for (var i = 0; i < values.length; i++) {"\
                      "    if(values[i].price >= high) {"\
                      "          high =  values[i].price ; }"\
                      "    if(values[i].price <= low) {"\
                      "          low =  values[i].price ; }"\
                      "  }"\
                      "  range = high - low;"\
                      "  return {high: high,low:low,range:range};"\
                      "}")
    return reduce
def GetQuery(startTime,endTime):
     query = {"date": {"$gte": startTime, "$lte" : endTime}}
     return query
"""Returns a query that gets all the data for a symbol based on the session defined in the symbol
    if the startTime or endTime is defined that value is used to define the range instead
"""
def GetQueryForSymbol(date,symbol,startTime = None,endTime = None):
    startDate = datetime.datetime(year=date.year,month=date.month,day=date.day,hour=0,minute=0,second=0)
    endDate = datetime.datetime(year=date.year,month=date.month,day=date.day,hour=23,minute=59,second=59)
    
    if(startTime == None ):
        startTime = TotalElapsedSeconds(symbol.startTime)
    if(endTime == None):
        endTime = TotalElapsedSeconds(symbol.endTime)

    query = {"start_date" :
                {"$gte" : startDate, 
                "$lte" : endDate } ,
            "end_date" :
                {"$gte" : startDate ,
                "$lte" : endDate }  ,
            "end_time" :
                { "$lte" : endTime } ,
            "start_time" :
                { "$gte" : startTime }
            } 
    return query
def TotalElapsedSeconds(time):
    return (time.hour * 3600) + (time.minute * 60) + (time.second)
    
"""imports the fileName into a collection
"""
def ProcessFile(fileName,date,symbol):
    """Takes a file name and inserts the tick data into the database"""
    from time import mktime
    from datetime import datetime
    
    Myfile = open("./" + fileName)
    logging.info("Importing data for %s for date: %s",es.ticker,date)
    conn = GetConnection()
    coll = conn[GetCollectionName(symbol,date)]
    index = 0
    for line in Myfile:
        try:
            symbol, date, price, volume, bid, ask = line.split('\t')
            d = datetime.fromtimestamp(mktime(time.strptime(date,"%m/%d/%Y %H:%M:%S")))
            post = {'date': d
                    ,'bid': float(bid)
                    ,'ask': float(ask)
                    ,'p': float(price)
                    ,'v': int(volume)}
            coll.insert(post)
            index += 1
        except:
            logging.warning("Unexpected error: %s",sys.exc_info()[0]
)
    logging.info("Uploaded %s records",index)
    Myfile.close()
def GetConnection():
    from pymongo import Connection
    connection = Connection("mongodb://jermeyz:zeidner1@staff.mongohq.com:10031/ES_DATA")
    #connection = Connection()
    db = connection['ES_DATA']
    return db

def GetCollectionName(symbol, date):
    return symbol.ticker  + "-" + str(date.year) + "-" + str(date.month) + "-" + str(date.day)
def Get30MinuteBarCollection(symbol):
    return symbol.ticker+ "-30-minute-bars"
def GetStatsCollectionName(symbol):
    return symbol.ticker + "-STATS"
def GetStatsName(symbol,description,startTime=None,endTime=None):
    if(startTime == None):
        return symbol.ticker + "-"+ symbol.GetSessionTimeSpan() + "-" + description
    
    return symbol.ticker + "-"+ startTime.strftime("%H:%M:%S") + "-" + "-" + endTime.strftime("%H:%M:%S") + "-" + description
def DailyStats(date,symbol):

    conn = GetConnection()
    
    queryCollection = conn[Get30MinuteBarCollection(symbol)]
    statsCollection = conn[GetStatsCollectionName(symbol)]
    
    queryForTotalVolume = GetQueryForSymbol(date,symbol)
    result = queryCollection.map_reduce(GetMapForTotalVolume(),GetReduceForTotalVolume(),"myresult",query=queryForTotalVolume)

    totalVolume = result.find_one()
    statsCollection.insert({GetStatsName(symbol,"total_volume") : { 'date' : date , 'value' : totalVolume['value']['volume']} })

    conn.drop_collection("myresult")
    
    result = queryCollection.map_reduce(GetMapForRange(),GetReduceForRange(),"myresult",query=queryForTotalVolume)
    
    totalRange = result.find_one()
#    statsCollection.insert({GetStatsName(symbol,"total_range") : { 'date' : date ,
#                                                                  'range' : totalRange['value']['range'],
#                                                                  'high' : totalRange['value']['high'],
#                                                                  'low' :  totalRange['value']['low']} })
    statsCollection.insert({GetStatsName(symbol,"total_range") : GetRangeStatsDocument(date, totalRange['value']['range'], 
                                                                                                                                                        totalRange['value']['high'],
                                                                                                                                                        totalRange['value']['low'] )})

    
    for x in symbol.GetSessionRange(30,date):
        
        queryForRange = GetQueryForSymbol(date,
                                    symbol,
                                    startTime = TotalElapsedSeconds(x.rangeStartTime),
                                    endTime = TotalElapsedSeconds(x.rangeEndTime) )
        
        
        result  = queryCollection.map_reduce(GetMapForTotalVolume(),
                                             GetReduceForTotalVolume(),
                                             "myresult",
                                             query=queryForRange)
        totalVolume = result.find_one()
        if(totalVolume is not None):
            statsCollection.insert({GetStatsName(symbol,"total_volume",x.rangeStartTime,x.rangeEndTime) : {'date' : date ,'value' : totalVolume['value']['volume']}})
        
        conn.drop_collection("myresult")
            
        result2 = queryCollection.map_reduce(GetMapForRange(),
                                             GetReduceForRange(),
                                             "myresult2",
                                             query=queryForRange)
        
        range = result2.find_one()
        rangeData =(CreateRangeData(range,date,symbol,x.rangeStartTime,x.rangeEndTime))
        pprint(rangeData)
        if(rangeData is not None):
            statsCollection.insert(rangeData)
        
        conn.drop_collection("myresult2")
    
def CreateRangeData(range,date,symbol,startTime,endTime):
    if(range is not None):
            if(range['value'].has_key("high")):  #if there is only one row for the query need to manually create the range
                return {GetStatsName(symbol,"total_range",startTime,endTime) :
                                   GetRangeStatsDocument(date,
                                                         range['value']['range'],
                                                         range['value']['high'],
                                                         range['value']['low'])}
            return {GetStatsName(symbol,"total_range",startTime,endTime) :
                                   GetRangeStatsDocument(date,
                                                         0,
                                                         range['value']['price'],
                                                         range['value']['price'])}
    return None
def GetRangeStatsDocument(date,range,high,low):
    return {  'date' : date ,
            'range' : range,
            'high' : high,
            'low' :  low} 
def CreateStatsForRangeOfMinutes(symbol,minutesPerBar,date):
    from datetime import timedelta
    
    conn = GetConnection()
    coll = conn[GetCollectionName(symbol,date)]
    rollup = conn[Get30MinuteBarCollection(symbol)]
   
    startTime = date + timedelta(hours=symbol.startTime.hour,minutes=symbol.startTime.minute,seconds=symbol.startTime.second )
    
    logging.info("creating %s minute data for %s on %s", minutesPerBar, symbol.ticker, date)

    for i in symbol.GetSessionRange(30,date): 
       
        rangeStartTime  = i.rangeStartTime 
        rangeEndTime = i.rangeEndTime#

        #print "start: %s end: %s" % (rangeStartTime,rangeEndTime)
       
        result = coll.map_reduce(GetMap(),GetReduce(),"myresult",query=GetQuery(rangeStartTime,rangeEndTime))
        #print result.count()
        for m in result.find():
            #print m
            #insert the volume at price for that given period
            doc = {'start_date': rangeStartTime,
                   'end_date': rangeEndTime,
                   'start_time' : TotalElapsedSeconds(rangeStartTime.time()),
                   'end_time' : TotalElapsedSeconds(rangeEndTime.time()),
                   'volume' : m['value']['volume'],
                   'price': m['_id'],
                   'bid' : m['value']['bid'],
                   'ask' : m['value']['ask']}
            rollup.insert(doc)
        conn.drop_collection("myresult")

def SetupLogging():
    logging.basicConfig(filename='log.txt',level=logging.DEBUG)
    
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)

es = Symbol("ES",datetime.time(hour=9,minute=30,second=00),datetime.time(hour=16,minute=15,second=00))
if __name__ == '__main__' :
    SetupLogging()
#    ProcessFile("test_data-1-2-2011.txt",datetime.datetime(2011,1,2),es)
#    ProcessFile("test_data-1-1-2011.txt",datetime.datetime(2011,1,1),es)
    #CreateStatsForRangeOfMinutes(es,30,datetime.datetime(2011,1,1))
    #CreateStatsForRangeOfMinutes(es,30,datetime.datetime(2011,1,2))
    DailyStats(datetime.datetime(2011,1,1),es)
    DailyStats(datetime.datetime(2011,1,2),es)
    #print GetCollectionName(es,datetime.datetime(2011,12,12))
    #print GetRange(es,30)
    
