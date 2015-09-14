# encoding: utf-8

describe ROM::Kafka::Connection::Consumer do

  # ============================================================================
  # We test not the poseidon API, but its proper usage by the Consumer.
  # That's why we stub poseidon classes.
  # ----------------------------------------------------------------------------
  let(:driver)     { Poseidon::PartitionConsumer }
  let(:connection) { double :connection }
  before { allow(driver).to receive(:consumer_for_partition) { connection } }
  # ============================================================================

  let(:consumer)   { described_class.new options }
  let(:brokers)    { ["127.0.0.1:9092", "127.0.0.2:9092"] }
  let(:client)     { "foo" }
  let(:topic)      { "bar" }
  let(:partition)  { 1 }
  let(:offset)     { 100 }
  let(:attributes) { { min_bytes: 2, max_bytes: 3000, max_wait_ms: 100 } }
  let(:tuple)      { { value: "Hi!", topic: "foo", partition: 1, offset: 100 } }
  let(:message)    { double :message, tuple }
  let(:options) do
    attributes.merge(
      client_id: client,
      brokers: brokers,
      topic: topic,
      partition: partition,
      offset: offset
    )
  end

  describe ".new" do
    subject { consumer }

    it { is_expected.to be_kind_of Enumerable }
  end # describe .new

  describe "#connection" do
    subject { consumer.connection }

    it "instantiates the driver" do
      expect(driver)
        .to receive(:consumer_for_partition)
        .with(client, brokers, topic, partition, offset, attributes)

      expect(subject).to eql(connection)
    end
  end # describe #connection

  describe "#fetch" do
    subject { consumer.fetch }

    before  { allow(connection).to receive(:fetch) { [message] } }

    it "fetches messages from a connection" do
      expect(connection).to receive(:fetch)
      expect(subject).to eql [tuple]
    end
  end # describe #fetch

  describe "#each" do

    let(:messages) { [message, message] }
    before { allow(connection).to receive(:fetch) { [messages.pop].compact } }

    context "without a block" do
      subject { consumer.each }

      it { is_expected.to be_kind_of Enumerator }
    end

    context "with a block" do
      subject { consumer.to_a }

      it "fetches messages until received any" do
        expect(connection).to receive(:fetch).exactly(3).times
        expect(subject).to eq [tuple, tuple]
      end
    end
  end # describe #each

end # describe ROM::Kafka::Connection::Consumer
