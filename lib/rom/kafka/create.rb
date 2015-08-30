# encoding: utf-8

module ROM::Kafka

  # The namespace for Kafka-specific ROM commands
  #
  module Commands

    # The Kafka-specific implementation of ROM::Commands::Create
    #
    # @example
    #   ROM.use(:auto_registration)
    #   ROM.setup(:kafka, :consumer, "localhost:9092")
    #
    #   class Users < ROM::Relation[:kafka]
    #     dataset "log.users"
    #   end
    #
    #   class AddUsers < ROM::Commands::Create[:kafka]
    #   end
    #
    #   rom = ROM.finalize.env
    #   rom.relation(:users).using(max_wait_ms: 100).offset(10).to_a
    #   # => [{ value: "something", topic: "log", key: "users", offset: 10 }]
    #
    class Create < ROM::Commands::Create

      adapter :kafka

      # Sends tuples to the current topic/partition of Kafka
      #
      # @param (see ROM::Kafka::Dataset#publish)
      #
      # @return (see ROM::Kafka::Dataset#publish)
      #
      def execute(*tuples)
        dataset.publish(*tuples)
      end

      # Returns the new commmand where relation's dataset is updated
      # using given options
      #
      # @param [Hash] options
      #
      # @return [ROM::Kafka::Commands::Create]
      #
      def using(options)
        new relation.using(options)
      end

    end # class Create

  end # module Commands

end # module ROM::Kafka
