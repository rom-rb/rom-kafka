# encoding: utf-8

describe ROM::Kafka::Dataset do

  let(:dataset)    { described_class.new(*params) }
  let(:params)     { [role, client, topic, attributes] }

  let(:attributes) { { foo: :FOO, bar: :BAR } }
  let(:client)     { :client_id }
  let(:topic)      { :qux }
  let(:role)       { :producer }
  let(:builder)    { ROM::Kafka::Drivers }
  let(:fetch)      { double :fetch, each: :enumerable }
  let(:session) do
    double :session, fetch: fetch, publish: nil, close: nil, next_offset: 3
  end

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

  describe "#client" do
    subject { dataset.client }

    it "is initialized" do
      expect(subject).to eql(client)
    end
  end # describe #client

  describe "#topic" do
    subject { dataset.topic }

    it "is initialized" do
      expect(subject).to eql(topic)
    end
  end # describe #topic

  describe "#session" do
    subject { dataset.session }

    context "when the session is set" do
      let(:dataset) { described_class.new(*params, session) }

      it { is_expected.to eql session }
    end

    context "by default" do
      it "builds the session" do
        expect(builder)
          .to receive(:build)
          .with(role, attributes.merge(topic: topic))
        subject
      end

      it "sets the session" do
        expect(subject).to eql session
      end
    end
  end # describe #topic

  describe "#reset" do
    subject { dataset.reset(bar: :QUX, baz: :BAZ) }

    let(:updated_attributes) { { foo: :FOO, bar: :QUX, baz: :BAZ } }

    it "closes the session" do
      expect(session).to receive(:close)
      subject
    end

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
      expect(subject.attributes).to eql(updated_attributes)
    end

    it "rebuilds the session" do
      expect(builder)
        .to receive(:build)
        .with(role, updated_attributes.merge(topic: topic))
      subject
    end
  end # describe #reset

  describe "#update" do
    subject { dataset.update(bar: :QUX, baz: :BAZ) }

    let(:updated_attributes) { { foo: :FOO, bar: :QUX, baz: :BAZ } }

    it "doesn't close the session" do
      expect(session).not_to receive(:close)
      subject
    end

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
      expect(subject.attributes).to eql(updated_attributes)
    end

    it "doesn't rebuild the session" do
      dataset # builds the session for the first time
      expect(builder).not_to receive(:build)
      expect(subject.session).to eql(session)
    end
  end # describe #update

  describe "#each" do
    subject { dataset.each }

    it "is delegated to session#fetch" do
      expect(session).to receive(:fetch).with(attributes)
      expect(subject).to eql fetch.each
    end
  end # describe #each

  describe "#publish" do
    after { dataset.publish(*tuples) }

    let(:tuples) { [double, double] }

    it "is delegated to session" do
      expect(session).to receive(:publish).with(*tuples)
    end
  end # describe #publish

  describe "#next_offset" do
    subject { dataset.next_offset }

    it "is delegated to #session" do
      expect(session).to receive(:next_offset)
      expect(subject).to eql 3
    end
  end # describe #next_offset

end # describe ROM::Kafka::Dataset
