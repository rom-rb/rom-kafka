# encoding: utf-8

shared_examples :rom_environment do
  before do
    ROM.use :auto_registration
    ROM.setup(
      :kafka,
      "localhost:9092",
      client_id: "admin",
      # use the number of partition as a key
      partitioner: -> key, total { key.to_i % total }
    )
  end

  let(:rom) { ROM.env }
end
