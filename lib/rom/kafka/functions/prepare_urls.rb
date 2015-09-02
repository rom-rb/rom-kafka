# encoding: utf-8

module ROM::Kafka

  module Functions

    class << self

      # Extracts hash urls settings (hosts and port) from <gateway> params
      #
      # @example
      #   fn = Functions[:prepare_urls]
      #   fn["127.0.0.1:9092", hosts: ["127.0.0.2"], port: 9093, offset: 1]
      #   # => {
      #   #      host: "127.0.0.1",
      #   #      port: 9092,
      #   #      brokers: ["127.0.0.1:9092", "127.0.0.2:9093"],
      #   #      offset: 1
      #   #    }
      #
      # @param [Array] params
      #
      # @return [Hash]
      #
      def prepare_urls(*params)
        opts = hashify params.flatten
        urls = extract_urls(opts)
        url  = urls.first || {}

        opts.delete(:hosts)
        opts.merge(url).merge(brokers: urls.map { |url| url.values.join(":") })
      end

      private

      HOST = /^.+(?=\:\d{4,5})|.+/
      PORT = /(?!\:)\d{4,5}/

      def hashify(params)
        hash = params.last.is_a?(Hash) ? params.pop : {}
        hash[:hosts] = params + (hash[:hosts] || [])
        hash
      end

      def extract_urls(hash)
        port = hash[:port]
        hash[:hosts].map do |item|
          { host: item[HOST], port: (item[PORT] || port).to_i }
        end
      end

    end # eigenclass

  end # module Functions

end # module ROM::Kafka
