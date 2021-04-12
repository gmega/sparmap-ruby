require 'open-uri'
require 'sparmap'

URL_LIST = %w(
  https://en.wikipedia.org/wiki/WebCrawler
  https://en.wikipedia.org/wiki/University_of_Washington
  https://en.wikipedia.org/wiki/Starwave
  https://en.wikipedia.org/wiki/America_Online
  https://en.wikipedia.org/wiki/Global_Network_Navigator
  https://en.wikipedia.org/wiki/Excite
)

# Downloads these 6 URLs using 5 threads in "parallel" (as in, Ruby parallel)
titles = Sparmap::Pool.with_pool(workers: 5) do |pool|
  result = pool.imap_unordered(URL_LIST) do |url|
    URI.open(url) { |f| f.read[/<title>(.*)<\/title>/, 1] }
  end
  result.to_a
end

puts 'And the titles are...'
titles.each { |title| puts "- #{title}" }