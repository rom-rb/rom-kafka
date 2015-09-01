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
  #   users = rom.relation(:users)
  #   users.where(partition: 1).offset(0).limit(1).to_a
  #   # => [
  #   #      { value: "Andrew", topic: "users", partition: 1, offset: 0 }
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

    # Returns new relation with updated partition or key to fetch messages from
    #
    # @option attributes [Integer] :partition
    # @option attributes [#to_s] :key
    #
    # @return [ROM::Kafka::Relation]
    #
    def where(attributes)
      using attributes.select { |key| [:key, :partition].include? key }
    end

    # Returns new relation where dataset is updated with given attributes
    #
    # @param [Hash] attributes
    #
    # @return [ROM::Kafka::Relation]
    #
    def using(attributes)
      self.class.new dataset.using(attributes)
    end

  end # class Relation

end # module ROM::Kafka
