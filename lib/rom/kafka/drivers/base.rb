# encoding: utf-8

module ROM::Kafka

  module Drivers

    # The base class for Kafka drivers
    #
    # @api private
    #
    class Base

      include DSL::Attributes
      include Enumerable

      # The method is forbidden by default.
      #
      # Only concrete `Consumer` driver can get messages from Kafka.
      #
      # @raise [NotImplementedError]
      #
      def each
        fail NotImplementedError
          .new "The producer cannot fetch messages from a Kafka broker"
      end

      # The method is forbidden by default.
      #
      # Only concrete `Producer` driver can put messages to Kafka.
      #
      # @raise [NotImplementedError]
      #
      def send(*)
        fail NotImplementedError
          .new "The consumer cannot send messages to a Kafka broker"
      end

    end # class Base

  end # module Drivers

end # module ROM::Kafka
