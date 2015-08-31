# encoding: utf-8

module ROM::Kafka

  # The Kafka-specific implementation of ROM::Relation
  #
  # @example
  #   ROM.use(:auto_registration)
  #   ROM.setup(:kafka, :consumer, "localhost:9092")
  #
  #   class Users < ROM::Relation[:kafka]
  #     topic "users"
  #   end
  #
  #   rom = ROM.finalize.env
  #   rom.relation(:users).using(max_wait_ms: 100).where(partition: 1).to_a
  #   # => [
  #   #      { value: "something", topic: "users", partition: 1, offset: 0 },
  #   #      # ...
  #   #    ]
  #
  class Relation < ROM::Relation

    adapter :kafka

    # Kafka-specific alias for the ROM `.dataset` helper method.
    #
    # @param [#to_sym] name
    #
    # @return [undefined]
    #
    def self.topic(name)
      dataset(name)
    end

    # Returns new relation where dataset is updated with given attributes
    #
    # @option attributes [Integer] :max_bytes
    # @option attributes [Integer] :min_bytes
    # @option attributes [Integer] :max_wait_ms
    #
    # @return [ROM::Kafka::Relation]
    #
    def using(attributes)
      allowed = [:max_bytes, :min_bytes, :max_wait_ms]
      options = attributes.select { |key| allowed.include? key }

      self.class.new dataset.update(options)
    end

    # Returns new relation with updated offset attribute
    #
    # @param [Integer] value
    #
    # @return [ROM::Kafka::Relation]
    #
    def offset(value)
      self.class.new dataset.reset(offset: value)
    end

    # Returns new relation with updated partition to fetch messages from
    #
    # @option attributes [Integer] :partition
    #
    # @return [ROM::Kafka::Relation]
    #
    def where(attributes)
      allowed = [:key, :partition]
      options = attributes.select { |key| allowed.include? key }

      self.class.new dataset.reset(options)
    end

  end # class Relation

end # module ROM::Kafka
