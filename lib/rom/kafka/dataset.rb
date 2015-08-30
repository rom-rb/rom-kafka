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

    # @!attribute [r] session
    #
    # @return [ROM::Kafka::Drivers::Base]
    #   The established session to Kafka (either producer or consumer).
    #
    attr_reader :session

    # @!attribute [r] attributes
    #
    # @return [Hash] initialized attributes of the dataset
    #
    attr_reader :attributes

    # Initializes a partition with attributes from the gateway,
    # as well as topic, partition key and offset.
    #
    # @option (see ROM::Kafka::Gateway)
    # @option attributes [:producer, :consumer] :role
    #   The role of Kafka client
    # @option attributes [#to_s] :topic
    #   The name of the topic.
    # @option attributes [#to_s] :key
    #   The partition key
    # @option attributes [Integer] :offset
    #   The default offset (to be used by a consumer only)
    #
    # @api private
    #
    def initialize(attributes)
      @attributes = block_given? ? attributes.merge(yield) : attributes
      @session    = Drivers.build @attributes
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

    # Sends messages to the Kafka brokers
    #
    # Defined by the producer session only.
    #
    # @return [undefined]
    #
    # @raise [NotImplementedError] if a client of gateway is a consumer.
    #
    def send(*tuples)
      session.send(*tuples)
    end

    # Returns a new dataset with the same attributes, except for updated offset
    #
    # @param [Integer] value The new offset
    #
    # @return [ROM::Kafka::Dataset]
    #
    def offset(value)
      session.close
      self.class.new(attributes) { { offset: value } }
    end

  end # class Dataset

end # module ROM::Kafka
