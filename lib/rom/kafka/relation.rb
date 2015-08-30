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

    # Returns the new relation where dataset is updated with given options
    #
    # @param [Hash] options
    #
    # @return [ROM::Kafka::Relation]
    #
    def using(options)
      self.class.new dataset.using(options)
    end

    # Returns the new relation where dataset is updated with given offset
    #
    # @param [Integer] value
    #
    # @return [ROM::Kafka::Relation]
    #
    def offset(value)
      using(offset: value)
    end

  end # class Relation

end # module ROM::Kafka
