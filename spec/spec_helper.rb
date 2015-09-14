# encoding: utf-8

begin
  require "hexx-suit"
  Hexx::Suit.load_metrics_for(self)
rescue LoadError
  require "hexx-rspec"
  Hexx::RSpec.load_metrics_for(self)
end

# Loads the code under test
require "rom-kafka"

if (timeout = ENV["USE_TIMEOUT"].to_f)
  RSpec.configure do |config|
    config.around(:each) { |example| Timeout.timeout(timeout, &example) }
  end
end
