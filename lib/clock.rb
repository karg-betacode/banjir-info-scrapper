require File.expand_path('../../config/boot',        __FILE__)
require File.expand_path('../../config/environment', __FILE__)
require 'clockwork'

include Clockwork

every(6.minutes, 'Crawl BanjirInfo') { InfobanjirScrapperWorker.perform_async }
every(20.minutes, 'Crawl Astro Awani') { AwaniScrapperWorker.perform_async }
every(15.minutes, 'Crawl Google and Bernama News') { BahasaNewsScraper.perform_async }