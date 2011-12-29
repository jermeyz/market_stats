module StatsHelper
	def getdb()
		@db = Mongo::Connection.new(APP_CONFIG["host"], APP_CONFIG["port"]).db(APP_CONFIG["database"])
		@auth = @db.authenticate(APP_CONFIG["username"],APP_CONFIG["password"])
		@coll = @db["ES-30-minute-bars"]
		@coll.find.each { |doc| puts doc.inspect }
		@my_doc = @coll.find_one()
		@my_doc["volume"]
		#@my_doc.each {  |t| puts t}



		#"<p>Hello world</p>"
	end
	def test()
	end
end
