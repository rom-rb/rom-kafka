shared_examples :scholars_topic do
  let!(:rom) do
    options = {
      client_id: "admin",
      partitioner: proc { |key, total| key.to_i % total }
    }
    ROM.container(:kafka, "localhost:9092", options) do |config|
      config.use(:macros)

      config.relation(:scholars)
      config.commands(:scholars) do
        define(:create)
      end
    end
  end

  let(:scholars) { rom.relation(:scholars) }
  let(:insert)   { rom.command(:scholars).create }
end
