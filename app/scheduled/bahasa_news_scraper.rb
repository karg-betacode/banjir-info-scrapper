require 'open-uri'

class BahasaNewsScraper
  include Sidekiq::Worker
  sidekiq_options queue: "BahasaNewsScraper", retry: false

  def initialize
    load_sql_client
  end

  def perform
    bernama()
    google()
  end

  private 

  def show_status(msg)
    current_time = Time.now.strftime('%H:%M:%S')
    puts "[#{current_time}] #{msg}"
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

  def clean_url(url)
    url.split('url=')[1]
  end

  def determine_source(url)
    case url
    when /bharian/
      'Berita Harian'
    when /utusan/
      'Utusan Malaysia'
    when /sinarharian/
      'Sinar Harian'
    when /hmetro/
      'Harian Metro'
    else
      'unsupported'
    end
  end

  def process_page(page, type)
    content = "";
    case type
    when 'Berita Harian'
      content = page.css('.field-item').first.text
    when 'Utusan Malaysia'
      content = page.css('div.element.article div.clearfix').first.text
    when 'Sinar Harian'
      content = page.css('div[itemprop=articleBody]').first.text
    when 'Harian Metro'
      content = page.css('.field-item').first.text
    when 'Harakah Daily'
      content = page.css('div.content.clearfix').first.text
    else
    end

    { 'content' => content }
  end


  def bernama
    show_status "Crawling Bernama..."
    url     = 'http://www.bernama.com/bernama/v7/bm/ge/listgeneral.php?page=2'
    html    = open(url).read
    page    = Nokogiri::HTML(open(url))
    content = page.css('div.boxMainNewsTitle')
    results = []

    for news in content 
      title = news.css('a').text    

      next if not title.downcase.include? "banjir" 

      a   = news.css('a').first
      url = 'http://www.bernama.com/bernama/v7/bm/ge/' + a['href']
      news.search('div').remove
      description = news.content.gsub(/\n|\t/,'')
      location = ""
      location_match = /(\w+),/.match(description)
      
      unless location_match.nil?
        location = location_match[1]
      end

      content = scrap_bernama(url)
      results << { 
        'title' => title,
        'url'   => url,
        'timestamp' => DateTime.now,
        'description' => description,
        'source' => 'Bernama',
        'location' => location,
        'body' => content['text'],
        'image' => content['image'] 
      }
    end
      
    results.each do |berita|
      found = false
      result = @client.execute("SELECT ID FROM Items WHERE Url = '#{berita['url']}'")
      result.each { |row| found = true }

      unless found
        sql = "INSERT INTO Items (Title, Body, Timestamp, Source, Url, Location, Image)
              VALUES
              ('#{@client.escape(berita['title'])}',
              '#{@client.escape(berita['body'])}',
              '#{@client.escape(berita['timestamp'].strftime("%Y-%m-%d %H:%M:%S"))}',
              '#{@client.escape(berita['source'])}',
              '#{@client.escape(berita['url'])}',
              '#{@client.escape(berita['location'])}',
              '#{@client.escape(berita['image'])}'
              )"

        result = @client.execute(sql) 
        id = result.insert
        show_status "success for URL: #{berita['url']}"  if id
      end
    end

    nil
  end

  def scrap_bernama(url)
    image = ""
    page  = Nokogiri::HTML(open(url))  
    image_node = page.css('span#newsPic2 > img').first
    
    image = image_node['src'] if not image_node.nil?
    news_text = page.css('div.NewsText')
    news_text.css('span').remove
    
    content  = { 
      'image' => image,
      'text' => news_text.text
    }  

    content
  end


  def google()
    show_status "Crawling Google..."
    rss_feed = Feedjira::Feed.fetch_and_parse('https://news.google.com.my/news?q=banjir&hl=en&gl=my&authuser=0&gbv=1&um=1&ie=UTF-8&output=rss')
    news     = Array.new
    rss_feed.entries.each do |item|

      url          = clean_url(item.url)
      type         = determine_source(url)
      next if type == 'unsupported'

      # Get information from the RSS
      title        = item.title
      published_at = item.published.in_time_zone.strftime("%Y-%m-%d %H:%M:%S")
      decription   = item.summary
      
      show_status "Crawling: #{url}"

      page = Nokogiri::HTML(open(url))
      details = process_page(page, type)

      # Process some values
      news << { 'title' => title,
                'url'   => url,
                'timestamp' => published_at,
                'description' => '',
                'source' => type,
                'body' => details['content'] }
    end

    # Insert into DB
    news.each do |berita|
      found = false
      result = @client.execute("SELECT ID FROM Items WHERE Url = '#{berita['url']}'")
      result.each { |row| found = true }

      unless found
        sql = "INSERT INTO Items (Title, Body, Timestamp, Source, Url)
               VALUES
               ('#{@client.escape(berita['title'])}',
               '#{@client.escape(berita['body'])}',
               '#{@client.escape(berita['timestamp'].to_s)}',
               '#{@client.escape(berita['source'])}',
               '#{@client.escape(berita['url'])}')"

        result = @client.execute(sql)
        id = result.insert
        show_status "Success for URL: #{berita['url']}" if id
      end
    end
    nil
  end
end