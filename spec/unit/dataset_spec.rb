# encoding: utf-8

describe ROM::Kafka::Dataset do

  let(:gateway_class)  { ROM::Kafka::Gateway }
  let(:consumer_class) { ROM::Kafka::Connection::Consumer }
  let(:consumer)       { double :consumer }
  before { allow(consumer_class).to receive(:new) { consumer } }

  let(:dataset) { described_class.new(gateway, topic.to_sym) }
  let(:gateway) { gateway_class.new client_id: "foo" }
  let(:topic)   { "bar" }

  describe "#gateway" do
    subject { dataset.gateway }

    it "is initialized" do
      expect(subject).to eql gateway
    end
  end # describe #gateway

  describe "#topic" do
    subject { dataset.topic }

    it "is initialized" do
      expect(subject).to eql topic
    end
  end # describe #topic

  describe "#attributes" do
    subject { dataset.attributes }

    context "by default" do
      let(:attributes) do
        {
          partition: 0,
          offset: 0,
          limit: 0,
          min_bytes: gateway.min_bytes,
          max_bytes: gateway.max_bytes,
          max_wait_ms: gateway.max_wait_ms
        }
      end

      it "is taken from a gateway" do
        expect(subject).to eql attributes
      end
    end

    context "when options are set" do
      let(:dataset) { described_class.new(gateway, topic, attributes) }
      let(:attributes) do
        {
          partition: 1,
          offset: 2,
          limit: 10,
          min_bytes: 1_024,
          max_bytes: 10_240,
          max_wait_ms: 100
        }
      end

      it "is initialized" do
        expect(subject).to eql attributes
      end
    end
  end # describe #attributes

  describe "#producer" do
    subject { dataset.producer }

    it "is taken from #gateway" do
      expect(subject).to eql gateway.producer
    end
  end # describe #producer

  describe "#consumer" do
    subject { dataset.consumer }

    let(:dataset) { described_class.new(gateway, topic, attributes) }

    let(:attributes) do
      {
        partition: 1,
        offset: 2,
        limit: 0,
        min_bytes: 1_024,
        max_bytes: 10_240,
        max_wait_ms: 100
      }
    end

    let(:options) do
      attributes.merge(
        topic: topic,
        client_id: gateway.client_id,
        brokers: gateway.brokers
      )
    end

    it "is initialized with proper options" do
      expect(consumer_class).to receive(:new).with(options)
      expect(subject).to eql consumer
    end
  end # describe #consumer

  describe "#using" do
    subject { dataset.using(update) }

    let(:dataset) { described_class.new gateway, topic, min_bytes: 8 }
    let(:update)  { { partition: 1, offset: 2 } }

    it "builds new dataset" do
      expect(subject).to be_kind_of described_class
    end

    it "preserves gateway" do
      expect(subject.gateway).to eql(gateway)
    end

    it "preserves topic" do
      expect(subject.topic).to eql(topic)
    end

    it "updates attributes" do
      expect(subject.attributes).to eql(dataset.attributes.merge(update))
    end
  end # describe #using

  describe "#each" do
    subject { dataset.to_a }

    let(:consumer) { double :consumer, each: data.each }
    let(:data)     { %w(foo bar baz qux) }

    context "when limit isn't set" do
      it "is delegated to the consumer" do
        expect(subject).to eql data
      end

      it "yields limited number of times" do
        expect { |b| dataset.each(&b) }.to yield_control.exactly(4).times
      end
    end

    context "when limit is set" do
      let(:dataset) { described_class.new(gateway, topic, limit: 2) }

      it "is delegated to the consumer" do
        expect(subject).to eql data[0..1]
      end

      it "yields limited number of times" do
        expect { |b| dataset.each(&b) }.to yield_control.twice
      end
    end

    context "without a block" do
      subject { dataset.each }

      it { is_expected.to be_kind_of Enumerator }
    end
  end # describe #each

end # describe ROM::Kafka::Dataset
