# encoding: utf-8

module ROM::Kafka

  # Describes the connection to Kafka cluster
  #
  # This is a base abstract class for producer and concumer connections.
  #
  # @api private
  #
  # @author Andrew Kozin <Andrew.Kozin@gmail.com>
  #
  class Connection

    extend AttributesDSL

    require_relative "connection/producer"
    require_relative "connection/consumer"

  end # class Connection

end # module ROM::Kafka
