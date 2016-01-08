describe ROM::Kafka::Connection::Producer do

  # ============================================================================
  # We test not the poseidon API, but its proper usage by the Producer.
  # That's why we stub poseidon classes.
  # ----------------------------------------------------------------------------
  let(:driver)     { Poseidon::Producer }
  let(:message)    { Poseidon::MessageToSend }
  let(:connection) { double :connection, send_messages: nil }

  before do
    allow(driver).to receive(:new) { connection }

    allow(message).to receive(:new) do |*args|
      double :message, to_tuple: Hash[[:topic, :value, :key].zip(args)]
    end
  end
  # ============================================================================

  let(:producer) { described_class.new options }
  let(:client)   { "foo" }
  let(:brokers)  { %w(127.0.0.1:9092 127.0.0.2:9093) }
  let(:options)  { attributes.merge(brokers: brokers, client_id: client) }
  let(:attributes) do
    {
      partitioner: proc { |v, c| v % c },
      type: :sync,
      compression_codec: :gzip,
      metadata_refresh_interval_ms: 100,
      max_send_retries: 3,
      retry_backoff_ms: 300,
      required_acks: 2,
      ack_timeout_ms: 200,
      socket_timeout_ms: 400
    }
  end

  describe "#connection" do
    subject { producer.connection }

    it "instantiates the driver" do
      expect(driver).to receive(:new).with(brokers, client, attributes)
      expect(subject).to eql(connection)
    end
  end

  describe "#publish" do
    subject { producer.publish(*input) }

    let(:input) do
      [
        { value: "foo", topic: "foos", key: 1 },
        [{ value: "bar", topic: "bars" }]
      ]
    end

    let(:output) do
      [
        { value: "foo", topic: "foos", key: "1" },
        { value: "bar", topic: "bars", key: nil }
      ]
    end

    it "builds messages and sends it to the #connection" do
      expect(connection).to receive(:send_messages) do |args|
        expect(args.map(&:to_tuple)).to eql output
      end

      subject
    end

    it "returns the plain array of tuples" do
      expect(subject).to eql output
    end
  end
end
