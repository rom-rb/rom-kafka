# encoding: utf-8

module ROM::Kafka

  # Describes the gateway to Kafka
  #
  # The gateway is responcible for registering datasets, and
  # storing attributes used by relations and commands
  # to connect to Kafka broker(s).
  #
  # The gateway by itself doesn't connect to Kafka because the parameters
  # like +:topic+ and +:partition+, that are necessary to establish
  # a connection, are dataset-specific. Moreover, the parameter +:offset+
  # (which is also used by a connection) in ROM is considered
  # a part of request to dataset.
  #
  class Gateway < ROM::Gateway

    include DSL::Attributes

    # Attributes used by both producer and consumer
    attribute :hosts
    attribute :port, default: 9092
    attribute :client

    # Producer-specific attributes
    attribute :async, default: false
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

    # @!attribute [r] role
    #
    # @return [:producer, :consumer] The role of the gateway's client
    #
    attr_reader :role

    # Initializes the gateway to Kafka with the role of client and
    # role-specific hash of attributes.
    #
    # The initializer is attributes-agnostic. This means it doesn't validate
    # attributes, but skips unused ones. That's why you can send the same
    # hash of attributes to both consumer and producer gateways, and leave
    # them sort the attributes by themselves.
    #
    # @example Initialize a producer's gateway to Kafka
    #   gateway = Gateway.new(
    #     :producer,
    #     hosts: ["127.0.0.1", "127.0.0.2"],
    #     port: 9092,
    #     compression-codec: :gzip
    #   )
    #
    # @example Alternative syntax
    #   gateway = Gateway.new(
    #     :producer,
    #     "127.0.0.1:9092",
    #     "127.0.0.2:9092",
    #     compression-codec: :gzip
    #   )
    #
    # @example Initialize a consumer's gateway to Kafka
    #   gateway = Gateway.new(
    #     :consumer,
    #     "127.0.0.1",
    #     port: 9092,
    #     min_bytes: 1024 # wait until 1Kb of messages is prepared
    #   )
    #
    # @param [:consumer, :producer] role
    #   The role of the Kafka client
    # @param [Hash] attributes
    #   The list of attributes, that is different for producer and consumer
    #
    # @option attributes [String, Array<String>] :hosts
    #   A host or list of hosts in the form "host1:port1" or "host1".
    #   In case of a consumer, only the first host is actually used.
    # @option attributes [Integer] :port
    #   The port shared by all hosts.
    # @option attributes [String] :client
    #   A client_id used to indentify the client (either producer or consumer).
    #
    # @option attributes [Boolean] :async (false)
    #   Whether messages should be quered and sent in the background.
    # @option attributes [:gzip, :snappy, nil] :compression_codec (nil)
    #   Type of compression to be used.
    # @option attributes [Integer] :metadata_refresh_interval_ms (600_000)
    #   How frequently the topic metadata should be updated (in milliseconds).
    # @option attributes [Integer] :max_send_retries (3)
    #   Number of times to retry sending of messages to a leader.
    # @option attributes [Integer] :retry_backoff_ms (100)
    #   The amount of time (in milliseconds) to wait before refreshing
    #   the metadata after we are unable to send messages.
    # @option attributes [Integer] :required_acks (0)
    #   The number of acks required per request.
    # @option attributes [Integer] :ack_timeout_ms (1_500)
    #   How long the producer waits for acks.
    # @option attributes [Integer] :socket_timeout_ms (10_000)
    #   How long the producer/consumer socket waits for any reply from server.
    #
    # @option attributes [Integer] :min_bytes (1)
    #   The smallest amount of data the server should send.
    #   (By default send us data as soon as it is ready).
    # @option attributes [Integer] :max_bytes (1_048_576)
    #   The maximum number of bytes to fetch by consumer (1MB by default).
    # @option attributes [Integer] :max_wait_ms (100)
    #   How long to block until the server sends data.
    #   NOTE: This is only enforced if min_bytes is > 0.
    #
    def initialize(role, *options)
      super extract_attributes(options)
      @role     = role
      @datasets = {}
    end

    # Returns the registered dataset by name
    #
    # @param [#to_sym] name
    #
    # @return [ROM::Kafka::Dataset]
    #
    def [](name)
      @datasets[name.to_sym]
    end

    # Registers the dataset by name
    #
    # The name should consist both the topic name and partition key, divided
    # by a colon (`:`). The key can include dots, but not a colons.
    #
    # @example
    #   dataset "logs:my.beloved.users"
    #
    # @param [#to_sym] name The full name of the dataset in dot notation.
    #
    # @return [self] itself
    #
    def dataset(name)
      topic, key = name.to_s.split(":")
      @datasets[name.to_sym] = Dataset.new(dataset_attributes(topic, key))
      self
    end

    # Checks whether a dataset is registered by name
    #
    # @param [#to_sym] name
    #
    # @return [Boolean]
    #
    def dataset?(name)
      self[name] ? true : false
    end

    private

    def dataset_attributes(topic, key)
      attributes.merge(role: role, topic: topic, key: key, offset: 0)
    end

    def extract_attributes(options)
      attributes = options.last.instance_of?(Hash) ? options.pop : {}
      { hosts: options }.merge(attributes)
    end

  end # class Gateway

end # module ROM::Kafka
