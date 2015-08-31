# encoding: utf-8

describe ROM::Kafka::Relation do

  let(:relation) { described_class.new dataset }
  let(:updated_dataset) { double :updated_dataset }
  let(:dataset) do
    double :dataset, update: updated_dataset, reset: updated_dataset
  end

  describe ".adapter" do
    subject { described_class.adapter }

    it { is_expected.to eql(:kafka) }
  end # describe .adapter

  describe ".new" do
    subject { relation }

    it { is_expected.to be_kind_of ROM::Relation }
  end # describe .new

  describe "#dataset" do
    subject { relation.dataset }

    it { is_expected.to eql(dataset) }
  end # describe #dataset

  describe "#using" do
    subject { relation.using(options.merge(foo: :bar)) }

    let(:options) { { max_bytes: 100, min_bytes: 100, max_wait_ms: 100 } }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates the dataset with allowed options only" do
      expect(dataset).to receive(:update).with(options)
      expect(subject.dataset).to eql(updated_dataset)
    end
  end # describe #using

  describe "#offset" do
    subject { relation.offset(value) }

    let(:value) { 101 }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates the dataset with given offset" do
      expect(dataset).to receive(:reset).with(offset: value)
      expect(subject.dataset).to eql(updated_dataset)
    end
  end # describe #offset

  describe "#where" do
    subject { relation.where(key: :foo, partition: 3, foo: :bar) }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates the dataset with key and partition" do
      expect(dataset).to receive(:reset).with(key: :foo, partition: 3)
      expect(subject.dataset).to eql(updated_dataset)
    end
  end # describe #where

end # describe ROM::Kafka::Relation
