class HomeController < ApplicationController
  before_action :load_sql_client

  def index
    feeds = get_feeds
    @timestamp = feeds.last_modified

    unless xxx_entry_exists(feeds)
      feeds.entries.each do |f|
        sql = generate_insert_query
        @client.execute(sql)
      end
    end
  end

  private
  def get_feeds
    url = "http://infobanjir.water.gov.my/infobanjir_rss.xml"
    feeds = Feedjira::Feed.fetch_and_parse url
  end

  def load_sql_client
    @client = TinyTds::Client.new (
      adapter: ENV["SQL_ADAPTER"],
      host: ENV["SQL_HOST"],
      database: ENV["SQL_DATABASE"],
      username: ENV["SQL_USERNAME"],
      password: ENV["SQL_PASSWORD"]
    )
  end

  def generate_insert_query(feed_entry)
    sql_arr = ["insert into Items(Title, Body, Description, Slug, Timestamp, Source, Url, Location) values (?,?,?,?,?,?,?,?)"]
    sql_arr << f.title    #Title
    sql_arr << nil        #Description
    sql_arr << @timestamp #Slug
    sql_arr << @timestamp #Timestamp
    sql_arr << feeds.url  #Source
    sql_arr << f.url      #Url
    sql_arr << nil        #Location
    make_sql_safe(sql_arr)
  end

  def make_sql_safe(sql_query_wit_parameters)
    ActiveRecord::Base.send(:sanitize_sql_array, sql_query_wit_parameters)
  end

  def xxx_entry_exists(rss_feed)
    last_modified = rss_feed.last_modified.strftime("%Y%m%d %r")
    query_array = ["select count(*) from Items where timestamp = ? and source = ?", last_modified, rss_feed.url]
    query = make_sql_safe(query_array)

    @client.execute(query_array).each do |result|
      return result[0] > 0
    end
  end
end
