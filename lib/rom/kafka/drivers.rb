# encoding: utf-8
require "poseidon"

module ROM::Kafka

  # Isolates external drivers from the rest of the gem
  #
  module Drivers

    require_relative "drivers/base"

  end # module Driver

end # module ROM::Kafka
