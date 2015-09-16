# encoding: utf-8

module Test # namespace for classes

  shared_examples :scholars_topic do
    before do
      ROM.use :auto_registration
      ROM.setup(
        :kafka,
        "localhost:9092",
        client_id: "admin",
        # use the number of partition as a key
        partitioner: -> key, total { key.to_i % total }
      )

      class Scholars < ROM::Relation[:kafka]
        topic :scholars # the same as `dataset`
        register_as :scholars
      end

      class AddScholars < ROM::Commands::Create[:kafka]
        relation :scholars
        register_as :insert
      end

      ROM.finalize
    end

    after do
      %w(AddScholars Scholars).each { |const| Test.send :remove_const, const }
    end

    let(:rom) { ROM.env }
  end # shared_examples

end # module Test
