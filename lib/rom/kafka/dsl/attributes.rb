# encoding: utf-8

module ROM::Kafka

  module DSL

    # Features to declare attributes and initialize them from hash
    #
    module Attributes

      # Class method helpers for declaring attributes
      #
      module ClassMethods

        # Returns the default hash of declared attributes
        #
        # @return [Hash<Symbol => Object>]
        #
        def attributes
          @attributes ||= {}
        end

        # Declares the attribute with an optional default value
        #
        # @param [Symbol] name
        # @options options [Object] :default
        #
        # @return [undefined]
        #
        def attribute(name, options = {})
          define_method(name) { attributes.fetch(name) }
          attributes[name] = options[:default]
        end

      end # module ClassMethods

      # @!attribute [r] attributes
      #
      # @return [Hash] initialized attributes
      #
      attr_reader :attributes

      # Initializes attributes from hash
      #
      # @param [Hash<Symbol => Object>] attributes
      #
      def initialize(attributes = {})
        default  = self.class.attributes
        assigned = attributes.select { |key| default.include? key }

        @attributes = default.merge assigned
      end

      # @private
      def self.included(klass)
        klass.__send__ :extend, ClassMethods
      end

    end # module Attributes

  end # module DSL

end # module ROM::Kafka
