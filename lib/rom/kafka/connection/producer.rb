# encoding: utf-8

module ROM::Kafka

  class Connection

    # The producer-specific connection to Kafka cluster
    #
    # It is wrapped around `Poseidon::Producer` driver, and responsible for
    # adopting poseidon API to ROM::Gateway via [#initializer] and [#publish]
    # methods.
    #
    # ROM::Kafka producer deals with tuples, hiding poseidon-specific
    # implementation of messages from the rest of the gem.
    #
    # @api private
    #
    class Producer < Connection

      # The 'poseidon' class describing a producer
      #
      # @return [Class]
      #
      DRIVER = Poseidon::Producer

      # The 'poseidon' class describing a message acceptable by producer
      #
      # @return [Class]
      #
      MESSAGE = Poseidon::MessageToSend

      # Attributes, acceptable by the `Poseidon::Producer` driver
      attribute :partitioner
      attribute :type, default: :sync
      attribute :compression_codec
      attribute :metadata_refresh_interval_ms
      attribute :max_send_retries
      attribute :retry_backoff_ms
      attribute :required_acks
      attribute :ack_timeout_ms
      attribute :socket_timeout_ms

      # @!attribute [r] connection
      #
      # @return [ROM::Kafka::Connections::Producer::DRIVER] driver to Kafka
      #
      attr_reader :connection

      # Initializes a producer connection
      #
      # The initializer is attributes-agnostic. This means it doesn't validate
      # attributes, but skips unused.
      #
      # @option options [#to_s] :client_id
      #   A required unique id used to indentify the Kafka client.
      # @option options [Array<String>] :brokers
      #   A list of seed brokers to find a lead broker to fetch messages from.
      # @option options [Proc, nil] :partitioner
      #   A proc used to provide partition from given key.
      # @option options [:gzip, :snappy, nil] :compression_codec (nil)
      #   Type of compression to be used.
      # @option options [Integer] :metadata_refresh_interval_ms (600_000)
      #   How frequently the topic metadata should be updated (in milliseconds).
      # @option options [Integer] :max_send_retries (3)
      #   Number of times to retry sending of messages to a leader.
      # @option options [Integer] :retry_backoff_ms (100)
      #   An amount of time (in milliseconds) to wait before refreshing
      #   the metadata after we are unable to send messages.
      # @option options [Integer] :required_acks (0)
      #   A number of acks required per request.
      # @option options [Integer] :ack_timeout_ms (1_500)
      #   How long the producer waits for acks.
      # @option options [Integer] :socket_timeout_ms (10_000)
      #   How long the producer/consumer socket waits for any reply from server.
      #
      def initialize(options)
        super # takes declared attributes only, skipping brokers and client_id
        brokers    = options.fetch(:brokers)
        client     = options.fetch(:client_id)
        @connection = DRIVER.new(brokers, client, attributes)
      end

      # Sends tuples to the underlying connection
      #
      # @param [Array<Hash>] data
      #
      # @return [Array<Hash>] the list of published tuples
      #
      def publish(*data)
        tuples = data.flatten
        @connection.send_messages tuples.map(&method(:message))

        tuples
      end

      private

      def message(tuple)
        MESSAGE.new(*tuple.values_at(:topic, :value, :key))
      end

    end # class Producer

  end # class Connection

end # module ROM::Kafka
