# encoding: utf-8

describe ROM::Kafka::Relation do

  let(:relation) { described_class.new dataset }
  let(:dataset)  { double :dataset, using: updated }
  let(:updated)  { double :updated }

  describe ".adapter" do
    subject { described_class.adapter }

    it { is_expected.to eql(:kafka) }
  end # describe .adapter

  describe ".topic" do
    subject { described_class.topic :foo }

    it "is an alias for .dataset" do
      allow(described_class).to receive(:dataset)

      expect(described_class).to receive(:dataset).with(:foo)
      subject
    end
  end # describe .topic

  describe ".new" do
    subject { relation }

    it { is_expected.to be_kind_of ROM::Relation }
  end # describe .new

  describe "#dataset" do
    subject { relation.dataset }

    it { is_expected.to eql(dataset) }
  end # describe #dataset

  describe "#using" do
    subject { relation.using(options) }

    let(:options) { { foo: :bar, baz: :qux } }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates a dataset" do
      expect(dataset).to receive(:using).with(options)
      expect(subject.dataset).to eql(updated)
    end
  end # describe #using

  describe "#offset" do
    subject { relation.offset(value) }

    let(:value) { 5 }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates the dataset with given offset" do
      expect(dataset).to receive(:using).with(offset: value)
      expect(subject.dataset).to eql(updated)
    end
  end # describe #offset

  describe "#limit" do
    subject { relation.limit(value) }

    let(:value) { 3 }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates the dataset with given limit" do
      expect(dataset).to receive(:using).with(limit: value)
      expect(subject.dataset).to eql(updated)
    end
  end # describe #limit

  describe "#where" do
    subject { relation.where(key: :foo, partition: 3, foo: :bar) }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates the dataset with key and partition" do
      expect(dataset).to receive(:using).with(key: :foo, partition: 3)
      expect(subject.dataset).to eql(updated)
    end
  end # describe #where

end # describe ROM::Kafka::Relation
