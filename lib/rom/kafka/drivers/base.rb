# encoding: utf-8

module ROM::Kafka

  module Drivers

    # The base class for Kafka drivers
    #
    # @api private
    #
    # @abstract
    #
    class Base

      include DSL::Attributes
      include Enumerable

      # Reads message(s) from Kafka.
      #
      # The method is forbidden by default.
      # Only concrete `Consumer` driver can get messages from Kafka.
      #
      # @return [Enumerator]
      #
      # @raise [NotImplementedError]
      #
      def each
        fail NotImplementedError
          .new "The producer cannot fetch messages from a Kafka broker"
      end

      # Writes message(s) to Kafka.
      #
      # The method is forbidden by default.
      # Only concrete `Producer` driver can put messages to Kafka.
      #
      # @return [undefined]
      #
      # @raise [NotImplementedError]
      #
      def publish(*)
        fail NotImplementedError
          .new "The consumer cannot publish messages to a Kafka broker"
      end

    end # class Base

  end # module Drivers

end # module ROM::Kafka
