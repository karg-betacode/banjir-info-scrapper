class InfoBanjirService
  def initialize
    @client = TinyTds::Client.new(
      adapter: ENV["SQL_ADAPTER"],
      host: ENV["SQL_HOST"],
      database: ENV["SQL_DATABASE"],
      username: ENV["SQL_USERNAME"],
      password: ENV["SQL_PASSWORD"]
    )
  end

  def get_last_20
    result_iter = @client.execute("select top 20 * from InfoBanjir order by ID desc")
    result = []
    result_iter.each do |r|
      result << r
    end
    result
  end
end