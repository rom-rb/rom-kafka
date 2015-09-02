# encoding: utf-8

describe ROM::Kafka::Dataset do

  let(:dataset)    { described_class.new role, client, topic, attributes }
  let(:attributes) { { foo: :FOO, bar: :BAR } }
  let(:client)     { :client_id }
  let(:topic)      { :qux }
  let(:role)       { :producer }
  let(:builder)    { ROM::Kafka::Drivers }
  let(:fetch)      { double :fetch }
  let(:session) do
    double :session, each: fetch, publish: nil, close: nil, next_offset: 3
  end

  before { allow(builder).to receive(:build) { session } }

  describe ".new" do
    subject { dataset }

    it "instantiates role" do
      expect(subject.role).to eql(role)
    end

    it "instantiates client" do
      expect(subject.client).to eql(client)
    end

    it "instantiates topic" do
      expect(subject.topic).to eql(topic)
    end

    it "instantiates attributes" do
      expect(subject.attributes).to eql(attributes)
    end
  end # describe #attributes

  describe "#using" do
    subject { dataset.using(bar: :QUX, baz: :BAZ) }

    let(:updated) { { foo: :FOO, bar: :QUX, baz: :BAZ } }

    it "builds new dataset" do
      expect(subject).to be_kind_of described_class
    end

    it "preserves role" do
      expect(subject.role).to eql(role)
    end

    it "preserves client" do
      expect(subject.client).to eql(client)
    end

    it "preserves topic" do
      expect(subject.topic).to eql(topic)
    end

    it "updates attributes" do
      expect(subject.attributes).to eql(updated)
    end
  end # describe #using

  describe "#each" do
    subject { dataset.each }

    it "builds a session" do
      dataset
      expect(builder)
        .to receive(:build)
        .with(role, attributes.merge(topic: topic, client_id: client))
      subject
    end

    it "is delegated to session" do
      expect(subject).to eql fetch
    end
  end # describe #each

  describe "#publish" do
    after { dataset.publish(*tuples) }

    let(:tuples) { [double, double] }

    it "builds a session" do
      dataset
      expect(builder)
        .to receive(:build)
        .with(role, attributes.merge(topic: topic, client_id: client))
    end

    it "is delegated to session" do
      expect(session).to receive(:publish).with(*tuples)
    end
  end # describe #publish

end # describe ROM::Kafka::Dataset
