# encoding: utf-8

module ROM::Kafka

  # Describes the gateway to Kafka
  #
  # The gateway has 3 responsibilities:
  # - registers the datasets describing various topics and partitions
  # - instantiates the producer connection to Kafka brokers that doesn't
  #   depend on a specific topic/partition settings
  # - stores settings for the consumer connections to Kafka, that
  #   depends on a specific topic/partition/offset
  #
  # Every dataset uses the same producer connection (defined by gateway) and
  # individual consumer's one. The consumer's connection is reloaded
  # every time the topic, partition or current offset is changed by a relation.
  #
  class Gateway < ROM::Gateway

    extend AttributesDSL

    # Attributes used by both producer and consumer
    attribute :client_id, required: true, &:to_s
    attribute :brokers

    # Producer-specific attributes
    attribute :partitioner
    attribute :compression_codec
    attribute :metadata_refresh_interval_ms, default: 600_000
    attribute :max_send_retries, default: 3
    attribute :retry_backoff_ms, default: 100
    attribute :required_acks, default: 0
    attribute :ack_timeout_ms, default: 1_500
    attribute :socket_timeout_ms, default: 10_000

    # Consumer-specific attributes
    attribute :min_bytes, default: 1
    attribute :max_bytes, default: 1_048_576
    attribute :max_wait_ms, default: 100

    # Initializes the gateway to Kafka broker(s).
    #
    # The initializer is attributes-agnostic. This means it doesn't validate
    # attributes, but skips unused.
    #
    # @example Initialize a producer's gateway to Kafka
    #   gateway = Gateway.new(
    #     hosts: ["127.0.0.1", "127.0.0.2:9093"],
    #     port: 9092,
    #     client_id: :my_user,
    #     compression-codec: :gzip
    #   )
    #   gateway.brokers # => ["127.0.0.1:9092", "127.0.0.2:9093"]
    #
    # @example Alternative syntax
    #   gateway = Gateway.new(
    #     "127.0.0.1:9092",
    #     "127.0.0.2:9093",
    #     client_id: :my_user,
    #     compression-codec: :gzip
    #   )
    #   gateway.brokers # => ["127.0.0.1:9092", "127.0.0.2:9093"]
    #
    # @example Mixed syntax
    #   gateway = Gateway.new(
    #     "127.0.0.1:9092",
    #     hosts: ["127.0.0.2"]
    #     port: 9093,
    #     client_id: :my_user,
    #     min_bytes: 1024 # wait until 1Kb of messages is prepared
    #   )
    #   gateway.brokers # => ["127.0.0.1:9092", "127.0.0.2:9093"]
    #
    # @param [nil, String, Array<String>] addresses
    #   The address(es) of broker(s) to connect (optional).
    #   Brokers can be alternatively set with `:hosts` and `:port` options.
    #
    # @option options [#to_s] :client_id
    #   A required unique id used to indentify the Kafka client.
    # @option options [String, Array<String>] :hosts
    #   A host or list of hosts in the form "host1:port1" or "host1".
    #   In case of a consumer, only the first host is actually used.
    # @option options [Integer] :port
    #   The port shared by all hosts.
    #
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
    def initialize(*addresses, **options)
      brokers = Brokers.new(addresses, options).to_a
      super options.merge(brokers: brokers) # prepares #attributes

      @producer = Connection::Producer.new(attributes)
      @datasets = {}
    end

    # @!attribute [r] producer
    #
    # @return [ROM::Kafka::Producer] the producer's connection to Kafka brockers
    #
    attr_reader :producer

    # Returns the registered dataset by topic
    #
    # @param [#to_sym] topic
    #
    # @return [ROM::Kafka::Dataset]
    #
    def [](topic)
      @datasets[topic.to_sym]
    end

    # Registers the dataset by topic
    #
    # By default the dataset is registered with 0 partition and 0 offset.
    # That settings can be changed from either relation of a command.
    #
    # @param [#to_sym] topic
    #
    # @return [self] itself
    #
    def dataset(topic)
      @datasets[topic.to_sym] = Dataset.new(self, topic)

      self
    end

    # Checks whether a dataset is registered by topic
    #
    # @param [#to_sym] topic
    #
    # @return [Boolean]
    #
    def dataset?(topic)
      self[topic] ? true : false
    end

  end # class Gateway

end # module ROM::Kafka
