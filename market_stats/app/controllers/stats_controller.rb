class StatsController < ApplicationController
  def index
 	db = Mongo::Connection.new(APP_CONFIG["host"],APP_CONFIG["port"]).db(APP_CONFIG["database"])
    	auth = db.authenticate(APP_CONFIG["username"],APP_CONFIG["password"])
    
    	@collection = db["ES-30-minute-bars"]

	@doc = @collection.find_one()

      respond_to do |format|
      	format.html # index.html.erb
      end
  end
end
