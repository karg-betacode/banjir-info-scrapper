require 'open-uri'

class AwaniScrapperWorker
  include Sidekiq::Worker
  sidekiq_options queue: "AwaniScrapperWorker", retry: false

  BAHASA_RSS_URL = "http://www.astroawani.com/rss/flood/feeds-magicmybanjir-6f1b2b17s8c559de155b"
  ENGLISH_RSS_URL = "http://english.astroawani.com/rss/flood/feeds-magicmybanjir-6f1b2b17s8c559de155b"

  def initialize
    load_sql_client
  end

  def perform
    bahasa_rss_feed = get_rss_feed BAHASA_RSS_URL
    new_records = to_insert_queries(bahasa_rss_feed)

    english_rss_feed = get_rss_feed ENGLISH_RSS_URL
    new_records + to_insert_queries(english_rss_feed)

    insert_records(new_records)
  end

  def insert_records(records)
    records.each do |record|
      show_status "Check if record exists: #{record[:url]}"
      unless record_exists?(record[:url])
        result = @client.execute(record[:sql])
        id = result.insert
        if id
          show_status "Inserted record with id: #{id} - #{record[:url]}"
        else
          show_status "Failed to insert: #{record[:url]}"
        end
      end
    end
  end

  private 
  def load_sql_client
    @client = TinyTds::Client.new(
      adapter: ENV["SQL_ADAPTER"],
      host: ENV["SQL_HOST"],
      database: ENV["SQL_DATABASE"],
      username: ENV["SQL_USERNAME"],
      password: ENV["SQL_PASSWORD"]
    )
  end

  def record_exists?(url)
    result = @client.execute "SELECT TOP 1 1 FROM Items WHERE Url = '#{url}'"
    !result.first.nil?
  end

  def to_insert_queries(rss_feed)

    new_records = []
    rss_feed.entries.each do |entry|
      record = generate_insert_query(entry)
      new_records << record
      show_status("Visited page: #{record[:url]}")
    end

    new_records
  end

  def generate_insert_query(feed_entry)
    content, image = get_content_and_image(feed_entry.url)

    timestamp = feed_entry.published.in_time_zone.strftime("%Y-%m-%d %H:%M:%S")
    sql = "insert into Items(Title, Body, Description, Timestamp, Source, Url, Image) 
          VALUES 
          ( '#{@client.escape(feed_entry.title)}', 
            '#{@client.escape(content)}', 
            '#{@client.escape(feed_entry.summary)}', 
            '#{@client.escape(timestamp)}', 
            '#{@client.escape('Astro Awani')}',
            '#{@client.escape(feed_entry.url)}',
            '#{@client.escape(image)}') "

    {
      url: feed_entry.url,
      sql: sql
    }
  end

  def get_content_and_image(url)
    page = Nokogiri::HTML(open(url))
    content = "";
    image = "";

    content = page.css('div.storytext').first
    content.css("br").each { |node| node.replace("\r\n") }

    image_tag = page.css('div#story_pic img')
    image = image_tag.first.attr('src') unless image_tag.first.nil?
    
    [content.text.strip, image]
  end

  def get_rss_feed(url)
    Feedjira::Feed.fetch_and_parse(url)
  end

  def show_status(msg)
    current_time = Time.now.strftime('%H:%M:%S')
    puts "[#{current_time}] #{msg}"
  end
end
