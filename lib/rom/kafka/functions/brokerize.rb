# encoding: utf-8

module ROM::Kafka

  module Functions

    class << self

      # Converts hosts and port settings into array of `:brokers`.
      #
      # @example
      #   fn = Functions[:brokerize]
      #   fn["127.0.0.1:9092", hosts: ["127.0.0.2"], port: 9093, offset: 1]
      #   # => {
      #   #      brokers: ["127.0.0.1:9092", "127.0.0.2:9093"],
      #   #      offset: 1
      #   #    }
      #
      # @param [Array] params
      #
      # @return [Hash]
      #
      def brokerize(*params)
        opts    = hashify params.flatten
        brokers = extract_brokers(opts)
        brokers = ["localhost:9092"] if brokers.empty?

        opts.merge(brokers: brokers).reject { |k| [:hosts, :port].include? k }
      end

      private

      HOST = /^.+(?=\:\d{4,5})|.+/
      PORT = /(?!\:)\d{4,5}/

      def hashify(params)
        hash = params.last.is_a?(Hash) ? params.pop : {}
        hash[:hosts] = params + (hash[:hosts] || [])
        hash
      end

      def extract_brokers(hash)
        port = hash[:port]
        hash[:hosts].map { |item| [item[HOST], (item[PORT] || port)].join(":") }
      end

    end # eigenclass

  end # module Functions

end # module ROM::Kafka
