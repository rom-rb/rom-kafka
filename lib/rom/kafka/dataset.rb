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

    # Initializes a partition with topic and attributes from the gateway.
    #
    # @param [#to_s] role
    # @param [#to_s] client
    # @param [#to_s] topic
    # @param [Hash] attributes
    #
    # @option (see ROM::Kafka::Gateway)
    #
    # @api private
    #
    def initialize(role, client, topic, attributes)
      @role = role
      @client = client
      @topic = topic
      @attributes = attributes
    end

    # Returns a new dataset with updated attributes and the same session
    #
    # @param [Hash] options The part of attributes to be updated
    #
    # @return [ROM::Kafka::Dataset]
    #
    def using(options)
      self.class.new(role, client, topic, attributes.merge(options))
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

    private

    def session
      Drivers.build(role, attributes.merge(topic: topic, client_id: client))
    end

  end # class Dataset

end # module ROM::Kafka
