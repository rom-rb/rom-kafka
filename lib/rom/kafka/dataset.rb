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

    # @!attribute [r] client
    #
    # @return [String] The id of Kafka client
    #
    attr_reader :client

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

    # @!attribute [r] session
    #
    # @return [ROM::Kafka::Drivers::Base] the current session to Kafka
    #
    attr_reader :session

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
    def initialize(role, client, topic, attributes, session = nil)
      @role = role
      @client = client
      @topic = topic
      @attributes = attributes
      @session = session || Drivers.build(role, attributes.merge(topic: topic))
    end

    # Returns a new dataset with updated attributes and the same session
    #
    # @param [Hash] options The part of attributes to be updated
    #
    # @return [ROM::Kafka::Dataset]
    #
    def update(options, conn = session)
      self.class.new(role, client, topic, attributes.merge(options), conn)
    end

    # Returns a new dataset with updated attributes and new session
    #
    # @param [Hash] options The part of attributes to be updated
    #
    # @return [ROM::Kafka::Dataset]
    #
    def reset(options)
      session.close
      update(options, nil)
    end

    # Publishes messages to the Kafka brokers
    #
    # Used by the producer session only.
    #
    # @param [Object, Array] messages The list of messages to be sent to Kafka
    #
    # @return [Array<Hash>] the list of tuples sent to Kafka
    #
    def publish(*messages)
      session.publish(*messages)
    end

    # Returns the enumerator to iterate via fetched tuples
    #
    # Used by the consumer session only.
    #
    # @yieldparam [Hash] tuple
    #
    # @return [Enumerator<Hash>]
    #
    def each
      session.fetch(attributes).each
    end

    # Returns the next offset for the consumer
    #
    # @return [Integer]
    #
    def next_offset
      session.next_offset
    end

  end # class Dataset

end # module ROM::Kafka
