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
      # @option opts [#to_s] :client_id
      #   A required unique id used to indentify the Kafka client.
      # @option opts [Array<String>] :brokers
      #   A list of seed brokers to find a lead broker to fetch messages from.
      # @option opts [String] :topic
      #   A name of the topic to fetch messages from.
      # @option opts [Integer] :partition
      #   A number of partition to fetch messages from.
      # @option opts [Integer] :offset
      #   An initial offset to start fetching from.
      # @option opts [Integer] :min_bytes (1)
      #   A smallest amount of data the server should send.
      #   (By default send us data as soon as it is ready).
      # @option opts [Integer] :max_bytes (1_048_576)
      #   A maximum number of bytes to fetch by consumer (1MB by default).
      # @option opts [Integer] :max_wait_ms (100)
      #   How long to block until the server sends data.
      #   NOTE: This is only enforced if min_bytes is > 0.
      #
      # @todo: refactor usinng factory method Connection.build_consumer
      def initialize(opts)
        super # takes declared attributes from options
        args = opts.values_at(:client_id, :brokers, :topic, :partition, :offset)
        @connection = DRIVER.consumer_for_partition(*args, attributes)
        @mutex = Mutex.new
      end

      # Fetches a single portion of messages and converts them to tuple
      #
      # @return [Array<Hash{Symbol => String, Integer}>]
      #
      def fetch
        @mutex.synchronize { @connection.fetch }.map(&method(:tuple))
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
          break unless tuples.any?
          tuples.each { |tuple| yield(tuple) }
        end
      end

      private

      def tuple(msg)
        { value: msg.value, topic: msg.topic, key: msg.key, offset: msg.offset }
      end
    end
  end
end
