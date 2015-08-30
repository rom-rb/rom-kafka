# encoding: utf-8

describe ROM::Kafka::Relation do

  let(:relation)        { described_class.new dataset }
  let(:dataset)         { double :dataset, using: updated_dataset }
  let(:updated_dataset) { double :updated_dataset }

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
    subject { relation.using(options) }

    let(:options) { { foo: :FOO, bar: :BAR } }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates the dataset" do
      expect(dataset).to receive(:using).with(options)
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
      expect(dataset).to receive(:using).with(offset: value)
      expect(subject.dataset).to eql(updated_dataset)
    end
  end # describe #offset

end # describe ROM::Kafka::Relation
