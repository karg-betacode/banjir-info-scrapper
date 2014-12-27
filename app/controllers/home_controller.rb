class HomeController < ApplicationController

  def index
    @result = InfoBanjirService.new().get_last_20
  end
end
