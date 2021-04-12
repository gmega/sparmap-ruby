# sparmap

A Simple PARallel MAP for Ruby. Based on the 
[Python multiprocessing API](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool) API,
implements an unordered map operation which runs on threads. 

## Installation

The easiest way to install this gem is using [Bundler](https://bundler.io/). Add this line to your application's Gemfile:

```ruby
gem 'sparmap', git: 'https://github.com/gmega/sparmap-ruby'
```

And then execute:

    $ bundle install

## Usage

`sparmap` exposes a simple API where you create a thread pool and then fire away your tasks. For example, 
we could download and extract the titles of HTML pages in parallel doing this:  

```ruby
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
```

Clearly, Ruby suffers from [GILitis](https://en.wikipedia.org/wiki/Global_interpreter_lock) much like Python, meaning 
threads are good for I/O bound workloads. Caveat emptor.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
