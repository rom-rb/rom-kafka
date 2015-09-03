# encoding: utf-8

require "transproc"

module ROM::Kafka

  # The collection of gem-speficic pure composable functions (transprocs)
  #
  module Functions

    extend Transproc::Registry

    require_relative "functions/brokerize"

  end # module Functions

end # module ROM::Kafka
