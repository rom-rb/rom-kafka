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
    #     dataset "users"
    #   end
    #
    #   class GreetUsers < ROM::Commands::Create[:kafka]
    #     relation :users
    #     register_as :greet
    #   end
    #
    #   rom = ROM.finalize.env
    #   rom.commands(:users).greet.where(partition: 1).call "Hi!"
    #   # => [{ value: "Hi!", topic: "users", key: "users", offset: 10 }]
    #
    class Create < ROM::Commands::Create

      adapter :kafka

      # Sends messages to the current topic/partition of Kafka
      #
      # @param (see ROM::Kafka::Dataset#publish)
      #
      # @return (see ROM::Kafka::Dataset#publish)
      #
      def execute(*messages)
        dataset.publish(*messages)
      end

    end # class Create

  end # module Commands

end # module ROM::Kafka
