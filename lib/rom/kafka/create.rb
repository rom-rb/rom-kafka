module ROM::Kafka
  # The namespace for Kafka-specific ROM commands
  #
  module Commands
    # The Kafka-specific implementation of ROM::Commands::Create
    #
    # @example
    #   ROM.use(:auto_registration)
    #   ROM.setup(:kafka, "localhost:9092")
    #
    #   class Users < ROM::Relation[:kafka]
    #     dataset :users
    #   end
    #
    #   class GreetUsers < ROM::Commands::Create[:kafka]
    #     relation :users
    #     register_as :greet
    #   end
    #
    #   rom = ROM.finalize.env
    #   greet = rom.commands(:users).greet
    #   greet.with(key: "greetings").call "Hi!"
    #   # => [{ value: "Hi!", topic: "users", key: "greetings" }]
    #
    class Create < ROM::Commands::Create

      adapter :kafka
      option  :key, reader: true

      # Sends messages to the current topic/partition of Kafka
      #
      # @param [#to_s, Array<#to_s>] messages
      #
      # @return [Array<Hash>]
      #
      def execute(*messages)
        tuples = messages.flatten.map(&method(:tuple))
        producer.publish(*tuples)
      end

      # Returns a new command where `:key` option is updated
      #
      # @param [Hash] options
      # @options options [Object] :key
      #   The key to be used by Kafka to define a partition
      #
      # @return [ROM::Kafka::Commands::Create]
      #
      def with(options)
        self.class.new relation, key: options.fetch(:key)
      end

      private

      def producer
        dataset.producer
      end

      def dataset
        relation.dataset
      end

      def tuple(text)
        { value: text.to_s, topic: dataset.topic, key: key }
      end
    end
  end
end
