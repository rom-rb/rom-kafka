# encoding: utf-8

module ROM::Kafka

  # The Kafka-specific implementation of ROM::Relation
  #
  # @example
  #   ROM.use(:auto_registration)
  #   ROM.setup(:kafka, "localhost:9092")
  #
  #   class Users < ROM::Relation[:kafka]
  #     topic "users"
  #   end
  #
  #   rom = ROM.finalize.env
  #   users = rom.relation(:users)
  #   users.where(partition: 1).offset(0).limit(1).to_a
  #   # => [
  #   #      { value: "Andrew", topic: "users", partition: 1, offset: 0 }
  #   #    ]
  #
  class Relation < ROM::Relation

    adapter :kafka
    forward :using

    # Kafka-specific alias for the ROM `.dataset` helper method.
    #
    # @param [#to_sym] name
    #
    # @return [undefined]
    #
    def self.topic(name)
      dataset(name)
    end

    # Returns new relation with updated `:partition` attribute
    #
    # @param [Integer] value
    #
    # @return [ROM::Kafka::Relation]
    #
    def from(value)
      using(partition: value)
    end

    # Returns new relation with updated `:offset` attribute
    #
    # @param [Integer] value
    #
    # @return [ROM::Kafka::Relation]
    #
    def offset(value)
      using(offset: value)
    end

    # Returns new relation with updated `:limit` attribute
    #
    # @param [Integer] value
    #
    # @return [ROM::Kafka::Relation]
    #
    def limit(value)
      using(limit: value)
    end

  end # class Relation

end # module ROM::Kafka
