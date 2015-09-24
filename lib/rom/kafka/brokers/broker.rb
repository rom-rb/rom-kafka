# encoding: utf-8

module ROM::Kafka

  class Brokers

    # Describes an address to a brocker
    #
    # @example
    #   broker = Broker.new host: "localhost:9092"
    #   broker.to_s # => "localhost:9092"
    #
    #   broker = Broker.new host: "localhost", port: 9092
    #   broker.to_s # => "localhost:9092"
    #
    #   broker = Broker.new host: "localhost:9092", port: 9093
    #   broker.to_s # => "localhost:9092"
    #
    # @author Andrew Kozin <Andrew.Kozin@gmail.com>
    #
    class Broker

      include Equalizer.new(:port, :host)
      include Immutability

      # Regex to extract host from address line
      HOST = %r{^\w+(\:\/\/)?\S+(?=\:)|\S+}.freeze

      # Regex to extract port from address line
      PORT = /(?!\:)\d{4,5}$/.freeze

      # @!attribute [r] host
      #
      # @return [String] the host of the broker
      #
      attr_reader :host

      # @!attribute [r] port
      #
      # @return [Integer] the port of the broker
      #
      attr_reader :port

      # Initializes a value object from host line and port
      #
      # @option options [#to_s] :host ("localhost")
      # @option options [#to_i] :port (9092)
      #
      def initialize(options = {})
        line  = options.fetch(:host) { "localhost" }
        @host = line[HOST]
        @port = (line[PORT] || options.fetch(:port) { 9092 }).to_i
      end

      # Returns the string representation of the broker in "host:port" format
      #
      # @return [String]
      #
      def to_s
        "#{host}:#{port}"
      end

    end # class Broker

  end # class Brokers

end # module ROM::Kafka
