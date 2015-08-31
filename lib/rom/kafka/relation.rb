# encoding: utf-8

module ROM::Kafka

  # The Kafka-specific implementation of ROM::Relation
  #
  # @example
  #   ROM.use(:auto_registration)
  #   ROM.setup(:kafka, :consumer, "localhost:9092")
  #
  #   class Users < ROM::Relation[:kafka]
  #     dataset "log.users"
  #   end
  #
  #   rom = ROM.finalize.env
  #   rom.relation(:users).using(max_wait_ms: 100).offset(10).to_a
  #   # => [{ value: "something", topic: "log", key: "users", offset: 10 }]
  #
  class Relation < ROM::Relation

    adapter :kafka

    # Returns the new relation where dataset is updated with given attributes
    #
    # @option attributes [Integer] :max_bytes
    # @option attributes [Integer] :min_bytes
    # @option attributes [Integer] :max_wait_ms
    #
    # @return [ROM::Kafka::Relation]
    #
    def using(attributes)
      reload(attributes, :max_bytes, :min_bytes, :max_wait_ms)
    end

    # Returns the new relation with updated offset attribute
    #
    # @param [Integer] value
    #
    # @return [ROM::Kafka::Relation]
    #
    def offset(value)
      reload({ offset: value }, :offset)
    end

    # Returns the new relation with updated partition to fetch messages from
    #
    # @option attributes [Integer] :partition
    #
    # @return [ROM::Kafka::Relation]
    #
    def where(attributes)
      reload(attributes, :key, :partition)
    end

    private

    def reload(options, *keys)
      attributes = options.select { |key| keys.include? key }
      self.class.new dataset.using(attributes)
    end

  end # class Relation

end # module ROM::Kafka
