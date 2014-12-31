class InfobanjirScrapperWorker
  include Sidekiq::Worker
  sidekiq_options queue: "InfobanjirScrapperWorker", retry: false

  def initialize
    load_sql_client
  end

  def perform
    rss_feed = get_rss_feed
    queries = []

    unless infobanjir_entry_exists(rss_feed)
      rss_feed.entries.each do |entry|
        sql = generate_insert_query(rss_feed, entry)
        queries << sql
        show_status "Going to insert record: #{sql}"
      end

      @client.execute(queries.join("; "))
      queries
    end
  end

  private

  def show_status(msg)
    current_time = Time.now.strftime('%H:%M:%S')
    puts "[#{current_time}] #{msg}"
  end


  def get_rss_feed
    url = "http://infobanjir.water.gov.my/infobanjir_rss.xml"
    Feedjira::Feed.fetch_and_parse(url)
  end

  def load_sql_client
    @client = TinyTds::Client.new(
      adapter: ENV["SQL_ADAPTER"],
      host: ENV["SQL_HOST"],
      database: ENV["SQL_DATABASE"],
      username: ENV["SQL_USERNAME"],
      password: ENV["SQL_PASSWORD"]
    )
  end

  def generate_insert_query(rss_feed, feed_entry)
    timestamp = get_rss_feed_timestamp(rss_feed)
    sql_arr = ["insert into InfoBanjir(Title, Body, Description, Slug, Timestamp, Source, Url, Location) values (?, ?, ?, ?, ?, ?, ?, ?)"]
    sql_arr << "Infobanjir - #{feed_entry.title}"     #Title
    sql_arr << feed_entry.summary   #Body
    sql_arr << nil                  #Description
    sql_arr << timestamp            #Slug
    sql_arr << timestamp            #Timestamp
    sql_arr << rss_feed.url         #Source
    sql_arr << "http://infobanjir.water.gov.my"       #Url
    sql_arr << feed_entry.title     #Location
    make_sql_safe(sql_arr)
  end

  def make_sql_safe(sql_query_with_parameters)
    ActiveRecord::Base.send(:sanitize_sql_array, sql_query_with_parameters)
  end

  def get_rss_feed_timestamp(rss_feed)
    rss_feed.last_modified.in_time_zone.strftime("%Y%m%d %r")
  end

  def infobanjir_entry_exists(rss_feed)
    last_modified = get_rss_feed_timestamp(rss_feed)
    query_array = ["select count(*) as counter from Items where timestamp = ? and source = ?", last_modified, rss_feed.url]
    query = make_sql_safe(query_array)

    @client.execute(query).first["counter"] > 0
  end
end