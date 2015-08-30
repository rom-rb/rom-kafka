# encoding: utf-8

module ROM::Kafka

  # The dataset describes a specific partition of some Kafka topic
  #
  # Dataset connects to Kafka via role-specific driver (depends on whether
  # the dataset is prepared for producer of consumer of Kafka messages).
  #
  # Every driver provides the same API with `#each`, `#send`, and `#close`.
  # Depending on the type of the driver (access to Kafka), it either
  # implements the corresponding method, or raises the exception.
  #
  # * a producer can only [#send] messages to Kafka;
  # * a consumer can only fetch messages and iterate (using [#each]) by them.
  #
  # The consumer can also change the initial [#offset] for selecting messages.
  #
  class Dataset

    include Enumerable

    # @!attribute [r] role
    #
    # @return [String] The role of Kafka client
    #
    attr_reader :role

    # @!attribute [r] topic
    #
    # @return [String] The name of the current topic
    #
    attr_reader :topic

    # @!attribute [r] attributes
    #
    # @return [Hash] initialized attributes of the dataset
    #
    attr_reader :attributes

    # Initializes a partition with topic and attributes from the gateway.
    #
    # @param [#to_s] topic
    #
    # @option (see ROM::Kafka::Gateway)
    # @option attributes [:producer, :consumer] :role
    #   The role of Kafka client
    # @option attributes [#to_s] :key
    #   The partition key
    # @option attributes [Integer] :offset
    #   The default offset (to be used by a consumer only)
    #
    # @api private
    #
    def initialize(role, topic, attributes)
      @role       = role
      @topic      = topic
      @attributes = attributes
    end

    # Returns the enumerator to iterate via fetched messages
    #
    # Defined by the consumer session only.
    #
    # @return [Enumerator]
    #
    # @raise [NotImplementedError] if a client of gateway is a producer.
    #
    def each
      session.each
    end

    # Publishes messages to the Kafka brokers
    #
    # Defined by the producer session only.
    #
    # @param [Hash, Array<Hash>] tuples The list of messages to be sent to Kafka
    #
    # @return [Array<Hash>] the list of messages sent to Kafka
    #
    # @raise [NotImplementedError] if a client of gateway is a consumer.
    #
    def publish(*tuples)
      session.publish(*tuples)
    end

    # Returns a new dataset with updated attributes
    #
    # @param [Hash] options The part of attributes to be updated
    #
    # @return [ROM::Kafka::Dataset]
    #
    def using(options)
      self.class.new(role, topic, attributes.merge(options))
    end

    private

    def session
      Drivers.build role, attributes.merge(topic: topic)
    end

  end # class Dataset

end # module ROM::Kafka
