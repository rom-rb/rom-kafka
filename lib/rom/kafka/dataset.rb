# encoding: utf-8

module ROM::Kafka

  # The dataset describes Kafka topic
  #
  # @api private
  #
  class Dataset

    extend AttributesDSL
    include Enumerable

    # Customizable attributes for a consumer connection
    attribute :partition, default: 0
    attribute :offset,    default: 0
    attribute :min_bytes
    attribute :max_bytes
    attribute :max_wait_ms

    # @!attribute [r] gateway
    #
    # @return [ROM::Kafka::Gateway]
    #   The back reference to the gateway, that provided the dataset
    #
    attr_reader :gateway

    # @!attribute [r] topic
    #
    # @return [String] The name of the topic, described by the dataset
    #
    attr_reader :topic

    # @!attribute [r] producer
    #
    # @return [ROM::Kafka::Connection::Producer]
    #   The producer connection to Kafka brokers, defined by a gateway.
    #   It is stored for being used by a `Create` command.
    #
    attr_reader :producer

    # @!attribute [r] consumer
    #
    # @return [ROM::Kafka::Connection::Consumer]
    #   The consumer connection to Kafka brokers, used to fetch messages
    #   via [#each] method call.
    #
    attr_reader :consumer

    # Initializes the dataset with a gateway and topic name
    #
    # Attributes are set by default from a gateway. Later you can create
    # new dataset for the same gateway and topic, but with attributes,
    # updated via [#using] method.
    #
    # @param [ROM::Kafka::Gateway] gateway
    # @param [String] topic
    #
    # @option options [Integer] :partition (0)
    #   A partition number to fetch messages from
    # @option options [Integer] :offset (0)
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
    def initialize(gateway, topic, **options)
      super gateway.attributes.merge(options)
      @topic    = topic
      @gateway  = gateway
      @producer = gateway.producer
      @consumer = prepare_consumer
    end

    # Returns a new dataset with updated consumer attributes
    #
    # @param [Hash] options The part of attributes to be updated
    #
    # @return [ROM::Kafka::Dataset]
    #
    def using(options)
      self.class.new(gateway, topic, attributes.merge(options))
    end

    # Returns the enumerator to iterate via tuples, fetched from a [#consumer].
    #
    # @yieldparam [Hash] tuple
    #
    # @return [Enumerator<Hash>]
    #
    def each
      consumer.each
    end

    private

    def prepare_consumer
      Connection::Consumer.new consumer_options
    end

    def consumer_options
      attributes.merge(
        topic: topic,
        client_id: gateway.client_id,
        brokers: gateway.brokers
      )
    end

  end # class Dataset

end # module ROM::Kafka
