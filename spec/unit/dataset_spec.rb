# encoding: utf-8

describe ROM::Kafka::Dataset do

  let(:dataset)    { described_class.new attributes }
  let(:attributes) { { foo: :FOO, bar: :BAR } }
  let(:session)    { double :session, each: nil, send: nil, close: nil }
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

  describe "#session" do
    subject { dataset.session }

    it "is initialized" do
      expect(builder).to receive(:build).with(attributes)
      expect(subject).to eql(session)
    end
  end # describe #session

  describe "#using" do
    subject { dataset.using(bar: :QUX, baz: :BAZ) }

    let(:updated_attributes) { { foo: :FOO, bar: :QUX, baz: :BAZ } }

    it "builds new dataset with updates attributes" do
      expect(subject).to be_kind_of described_class
      expect(subject.attributes).to eql(updated_attributes)
    end

    it "closes and re-builds the session" do
      expect(session).to receive(:close).ordered
      expect(builder).to receive(:build).with(updated_attributes).ordered
      subject
    end
  end # describe #using

  describe "#each" do
    after { dataset.each }

    it "is delegated to session" do
      expect(session).to receive(:each)
    end
  end # describe #each

  describe "#send" do
    after { dataset.send(*tuples) }

    let(:tuples) { [double, double] }

    it "is delegated to session" do
      expect(session).to receive(:send).with(*tuples)
    end
  end # describe #send

end # describe ROM::Kafka::Dataset
