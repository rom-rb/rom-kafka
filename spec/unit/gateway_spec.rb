# encoding: utf-8

describe ROM::Kafka::Gateway do

  let(:gateway) { described_class.new(client_id: :foo) }

  describe ".new" do
    context "without client id" do
      subject { gateway }

      it { is_expected.to be_kind_of ROM::Gateway }
    end

    context "without client id" do
      subject { described_class.new }

      it "fails" do
        expect { subject }.to raise_error ArgumentError
      end
    end
  end # describe .new

  describe "#brokers" do
    subject { gateway.brokers }

    let(:brokers) { %w(localhost:9092 127.0.0.1:9092) }

    context "from strings" do
      let(:gateway) { described_class.new(*brokers, client_id: :foo) }

      it { is_expected.to eql brokers }
    end

    context "from hosts and port options" do
      let(:gateway) do
        described_class
          .new(client_id: :foo, hosts: %w(localhost 127.0.0.1), port: 9092)
      end

      it { is_expected.to eql brokers }
    end

    context "from mixed options" do
      let(:gateway) do
        described_class
          .new("localhost", client_id: :foo, hosts: %w(127.0.0.1), port: 9092)
      end

      it { is_expected.to eql brokers }
    end
  end # describe #hosts

  describe "#attributes" do
    subject { gateway.attributes }

    context "by default" do
      let(:attributes) { { client_id: :foo } }

      it "is set" do
        expect(subject).to eql(
          ack_timeout_ms: 1_500,
          brokers: ["localhost:9092"],
          client_id: "foo",
          compression_codec: nil,
          max_bytes: 1_048_576,
          max_send_retries: 3,
          max_wait_ms: 100,
          metadata_refresh_interval_ms: 600_000,
          min_bytes: 1,
          partitioner: nil,
          required_acks: 0,
          retry_backoff_ms: 100,
          socket_timeout_ms: 10_000
        )
      end
    end

    context "when assigned" do
      let(:gateway) { described_class.new(attributes) }

      let(:attributes) do
        {
          ack_timeout_ms: 200,
          brokers: ["localhost:9092"],
          client_id: "foo",
          compression_codec: :gzip,
          max_bytes: 2_048,
          max_send_retries: 2,
          max_wait_ms: 300,
          metadata_refresh_interval_ms: 300_000,
          min_bytes: 1_024,
          partitioner: proc { |value, count| value % count },
          required_acks: 1,
          retry_backoff_ms: 200,
          socket_timeout_ms: 20_000
        }
      end

      it { is_expected.to eql attributes }
    end
  end # describe #attributes

  describe "#[]" do
    subject { gateway[:foo] }

    it { is_expected.to eql(nil) }
  end # describe #[]

  describe "#dataset?" do
    before do
      allow(gateway).to receive(:[]) { |name| { foo: :FOO }[name.to_sym] }
    end

    context "when dataset is registered" do
      subject { gateway.dataset? "foo" }

      it { is_expected.to eql true }
    end

    context "when dataset isn't registered" do
      subject { gateway.dataset? "bar" }

      it { is_expected.to eql false }
    end
  end # describe #dataset

  describe "#dataset" do
    subject { gateway.dataset topic }

    let(:klass)   { ROM::Kafka::Dataset }
    let(:dataset) { double :dataset }
    let(:topic)   { "foobar" }

    before { allow(klass).to receive(:new) { dataset } }

    it "builds a dataset" do
      expect(klass).to receive(:new).with(gateway, topic)
      subject
    end

    it "registers a dataset by symbol" do
      expect { subject }.to change { gateway[:foobar] }.from(nil).to(dataset)
    end

    it "registers a dataset by string" do
      expect { subject }.to change { gateway["foobar"] }.from(nil).to(dataset)
    end

    it "returns itself" do
      expect(subject).to eql(gateway)
    end
  end # describe #dataset

  describe "#producer" do
    subject { gateway.producer }
    let(:producer) { double :producer }

    it "builds a producer" do
      attributes = {}
      producer   = double :producer

      expect(ROM::Kafka::Connection::Producer).to receive(:new) do |opts|
        attributes = opts
        producer
      end
      expect(subject).to eql producer
      expect(attributes).to eql gateway.attributes
    end
  end # describe #producer

end # describe ROM::Kafka::Gateway
