# encoding: utf-8

describe ROM::Kafka::Dataset do

  let(:dataset)    { described_class.new role, topic, attributes }
  let(:attributes) { { foo: :FOO, bar: :BAR } }
  let(:topic)      { :qux }
  let(:role)       { :producer }

  let(:session)    { double :session, each: nil, publish: nil }
  let(:builder)    { ROM::Kafka::Drivers }

  before { allow(builder).to receive(:build) { session } }

  describe ".new" do
    subject { dataset }
  end # describe .new

  describe "#attributes" do
    subject { dataset.attributes }

    it "is initialized" do
      expect(subject).to eql(attributes)
    end
  end # describe #attributes

  describe "#role" do
    subject { dataset.role }

    it "is initialized" do
      expect(subject).to eql(role)
    end
  end # describe #role

  describe "#topic" do
    subject { dataset.topic }

    it "is initialized" do
      expect(subject).to eql(topic)
    end
  end # describe #topic

  describe "#using" do
    subject { dataset.using(bar: :QUX, baz: :BAZ) }

    let(:updated_attributes) { { foo: :FOO, bar: :QUX, baz: :BAZ } }

    it "builds new dataset" do
      expect(subject).to be_kind_of described_class
    end

    it "updates attributes" do
      expect(subject.attributes).to eql(updated_attributes)
    end

    it "preserves topic" do
      expect(subject.topic).to eql(topic)
    end

    it "preserves role" do
      expect(subject.role).to eql(role)
    end
  end # describe #using

  describe "#each" do
    after { dataset.each }

    it "is builds the session" do
      expect(builder).to receive(:build)
        .with(role, attributes.merge(topic: topic))
    end

    it "is delegated to session" do
      expect(session).to receive(:each)
    end
  end # describe #each

  describe "#publish" do
    after { dataset.publish(*tuples) }

    let(:tuples) { [double, double] }

    it "is builds the session" do
      expect(builder).to receive(:build)
        .with(role, attributes.merge(topic: topic))
    end

    it "is delegated to session" do
      expect(session).to receive(:publish).with(*tuples)
    end
  end # describe #publish

end # describe ROM::Kafka::Dataset
