shared_examples :scholars_topic do
  let!(:rom) do
    env = ROM::Environment.new
    env.use :auto_registration

    setup = env.setup(
      :kafka, "localhost:9092",
      client_id: "admin",
      # use the number of partition as a key
      partitioner: -> key, total { key.to_i % total }
    )

    setup.relation(:scholars)
    setup.commands(:scholars) do
      define(:create)
    end

    setup.finalize
    setup.env
  end

  let(:scholars) { rom.relation(:scholars) }
  let(:insert)   { rom.command(:scholars).create }
end
