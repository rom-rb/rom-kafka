begin
  require "hexx-suit"
  Hexx::Suit.load_metrics_for(self)
rescue LoadError
  require "hexx-rspec"
  Hexx::RSpec.load_metrics_for(self)
end

# Loads the code under test
require "rom-kafka"

# @todo Remove after resolving of mutant PR#444
# @see https://github.com/mbj/mutant/issues/444
if ENV["MUTANT"]
  RSpec.configure do |config|
    config.around { |example| Timeout.timeout(0.5, &example) }
  end
end

# Loads specific matchers
require "immutability/rspec"
