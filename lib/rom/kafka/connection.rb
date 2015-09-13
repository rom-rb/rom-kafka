# encoding: utf-8

module ROM::Kafka

  # Describes the connection to Kafka broker(s)
  #
  # This is a base abstract class for producer and concumer connections.
  #
  # @abstract
  # @api private
  #
  # @author Andrew Kozin <Andrew.Kozin@gmail.com>
  #
  class Connection

    extend AttributesDSL

    require_relative "connection/producer"

  end # class Connection

end # module ROM::Kafka
