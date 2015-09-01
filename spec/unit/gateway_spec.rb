# encoding: utf-8

describe ROM::Kafka::Gateway do

  let(:gateway) { described_class.new(*params) }
  let(:params)  { [:producer, :baz] }

  describe ".new" do
    subject { described_class.new(*params, {}) }

    it { is_expected.to be_kind_of ROM::Kafka::DSL::Attributes }
  end # describe .new

  describe "#attributes" do
    subject { gateway.attributes }

    it "returns default settings" do
      expect(subject).to eql(
        ack_timeout_ms: 1_500,
        async: false,
        compression_codec: nil,
        hosts: ["localhost"],
        max_bytes: 1_048_576,
        max_send_retries: 3,
        max_wait_ms: 100,
        metadata_refresh_interval_ms: 600_000,
        min_bytes: 1,
        partitioner: nil,
        port: 9092,
        required_acks: 0,
        retry_backoff_ms: 100,
        socket_timeout_ms: 10_000
      )
    end
  end # describe #attributes

  describe "#role" do
    subject { gateway.role }

    it "is initialized" do
      expect(subject).to eql(:producer)
    end
  end # describe #role

  describe "#client" do
    subject { gateway.client }

    it "is initialized" do
      expect(subject).to eql(:baz)
    end
  end # describe #client

  describe "#hosts" do
    subject { gateway.hosts }

    let(:hosts) { %w(localhost:9092 127.0.0.1) }

    context "from string" do
      let(:gateway) { described_class.new(*params, *hosts) }

      it { is_expected.to eql hosts }
    end

    context "from the hosts option" do
      let(:gateway) { described_class.new(*params, hosts: hosts) }

      it { is_expected.to eql hosts }
    end

    context "from mixed attributes" do
      let(:gateway) do
        described_class.new(*params, "127.0.0.1:9092", hosts: hosts)
      end

      it "prefers options" do
        expect(subject).to eql hosts
      end
    end
  end # describe #hosts

  describe "#ack_timeout_ms" do
    let(:gateway) { described_class.new(*params, ack_timeout_ms: 1_000) }

    it "is initialized" do
      expect(gateway.ack_timeout_ms).to eql(1_000)
    end
  end # describe #ack_timeout_ms

  describe "#async" do
    let(:gateway) { described_class.new(*params, async: true) }

    it "is initialized" do
      expect(gateway.async).to eql(true)
    end
  end # describe #async

  describe "#compression_codec" do
    let(:gateway) { described_class.new(*params, compression_codec: :gzip) }

    it "is initialized" do
      expect(gateway.compression_codec).to eql(:gzip)
    end
  end # describe #compression_codec

  describe "#max_bytes" do
    let(:gateway) { described_class.new(*params, max_bytes: 1_000) }

    it "is initialized" do
      expect(gateway.max_bytes).to eql(1_000)
    end
  end # describe #max_bytes

  describe "#max_send_retries" do
    let(:gateway) { described_class.new(*params, max_send_retries: 2) }

    it "is initialized" do
      expect(gateway.max_send_retries).to eql(2)
    end
  end # describe #max_send_retries

  describe "#max_wait_ms" do
    let(:gateway) { described_class.new(*params, max_wait_ms: 2_000) }

    it "is initialized" do
      expect(gateway.max_wait_ms).to eql(2_000)
    end
  end # describe #max_wait_ms

  describe "#metadata_refresh_interval_ms" do
    let(:gateway) do
      described_class.new(*params, metadata_refresh_interval_ms: 600)
    end

    it "is initialized" do
      expect(gateway.metadata_refresh_interval_ms).to eql(600)
    end
  end # describe #metadata_refresh_interval_ms

  describe "#min_bytes" do
    let(:gateway) { described_class.new(*params, min_bytes: 1_024) }

    it "is initialized" do
      expect(gateway.min_bytes).to eql(1_024)
    end
  end # describe #min_bytesms

  describe "#port" do
    let(:gateway) { described_class.new(*params, port: 9093) }

    it "is initialized" do
      expect(gateway.port).to eql(9093)
    end
  end # describe #port

  describe "#required_acks" do
    let(:gateway) { described_class.new(*params, required_acks: 1) }

    it "is initialized" do
      expect(gateway.required_acks).to eql(1)
    end
  end # describe #required_acks

  describe "#retry_backoff_ms" do
    let(:gateway) { described_class.new(*params, retry_backoff_ms: 200) }

    it "is initialized" do
      expect(gateway.retry_backoff_ms).to eql(200)
    end
  end # describe #retry_backoff_ms

  describe "#socket_timeout_ms" do
    let(:gateway) { described_class.new(*params, socket_timeout_ms: 1_000) }

    it "is initialized" do
      expect(gateway.socket_timeout_ms).to eql(1_000)
    end
  end # describe #socket_timeout_ms

  describe "#partitioner" do
    let(:gateway) { described_class.new(*params, partitioner: partitioner) }
    let(:partitioner) { double :partitioner }

    it "is initialized" do
      expect(gateway.partitioner).to eql(partitioner)
    end
  end # describe #partitioner

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
      expect(klass)
        .to receive(:new)
        .with(*params, topic, gateway.attributes)
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

end # describe ROM::Kafka::Gateway
