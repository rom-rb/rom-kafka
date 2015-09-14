# encoding: utf-8
require "attributes_dsl"
require "poseidon"
require "rom"

# Ruby Object Mapper
#
# @see http://rom-rb.org/
#
module ROM

  # Apache Kafka support for ROM
  #
  # @see http://kafka.apache.org/
  #
  module Kafka

    require_relative "kafka/brokers"
    require_relative "kafka/connection"
    require_relative "kafka/dataset"
    require_relative "kafka/gateway"
    require_relative "kafka/relation"
    require_relative "kafka/create"

  end # module Kafka

end # module ROM
