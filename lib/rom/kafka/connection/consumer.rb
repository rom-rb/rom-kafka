# encoding: utf-8

module ROM::Kafka

  class Connection

    # The consumer-specific connection to Kafka cluster
    #
    # It is wrapped around `Poseidon::Consumer` driver, and responsible for
    # adopting poseidon API to ROM::Dataset via [#initializer] and [#each]
    # methods.
    #
    # ROM::Kafka consumer deals with tuples, hiding poseidon-specific
    # implementation of fetched messages from the rest of the gem.
    #
    # @api private
    #
    class Consumer < Connection

      include Enumerable

      # The 'poseidon'-specific class describing consumers
      #
      # @return [Class]
      #
      DRIVER = Poseidon::PartitionConsumer

      # Attributes acceptable by the `Poseidon::Consumer` driver
      attribute :min_bytes
      attribute :max_bytes
      attribute :max_wait_ms

      # @!attribute [r] connection
      #
      # @return [ROM::Kafka::Connection::Consumer::DRIVER] driver to Kafka
      #
      attr_reader :connection

      # Initializes a consumer connection
      #
      # The initializer is attributes-agnostic. This means it doesn't validate
      # attributes, but skips unused.
      #
      # @option options [#to_s] :client_id
      #   A required unique id used to indentify the Kafka client.
      # @option options [Array<String>] :brokers
      #   A list of seed brokers to find a lead broker to fetch messages from.
      # @option options [String] :topic
      #   A name of the topic to fetch messages from.
      # @option options [Integer] :partition
      #   A number of partition to fetch messages from.
      # @option options [Integer] :offset
      #   An initial offset to start fetching from.
      # @option options [Integer] :min_bytes (1)
      #   A smallest amount of data the server should send.
      #   (By default send us data as soon as it is ready).
      # @option options [Integer] :max_bytes (1_048_576)
      #   A maximum number of bytes to fetch by consumer (1MB by default).
      # @option options [Integer] :max_wait_ms (100)
      #   How long to block until the server sends data.
      #   NOTE: This is only enforced if min_bytes is > 0.
      #
      def initialize(options)
        super # takes declared attributes from options
        args =
          options
          .values_at(:client_id, :brokers, :topic, :partition, :offset)
        @connection = DRIVER.consumer_for_partition(*args, attributes)
      end

      # Fetches a single portion of messages and converts them to tuple
      #
      # @return [Array<Hash{Symbol => String, Integer}>]
      #
      def fetch
        result = @connection.fetch
        result.map(&method(:tuple))
      end

      # Iterates through Kafka messages
      #
      # Fetches the next portion of messages until no messages given
      #
      # @return [Enumerator<Array<Hash{Symbol => String, Integer}>>]
      #
      # @yieldparam [Hash{Symbol => String, Integer}] tuple
      #
      def each
        return to_enum unless block_given?
        loop do
          tuples = fetch
          tuples.any? ? tuples.each { |tuple| yield(tuple) } : break
        end
      end

      private

      def tuple(message)
        {
          value: message.value,
          topic: message.topic,
          partition: message.partition,
          offset: message.offset
        }
      end

    end # class Consumer

  end # class Connection

end # module ROM::Kafka
