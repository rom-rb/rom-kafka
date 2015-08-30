# encoding: utf-8

describe ROM::Kafka::Commands::Create do

  let(:command)  { described_class.new relation }
  let(:relation) { double :relation, using: updated, dataset: dataset }
  let(:updated)  { double :updated }
  let(:dataset)  { double :dataset, publish: tuples }
  let(:tuples)   { [double(:first_message), double(:second_message)] }

  describe ".adapter" do
    subject { described_class.adapter }

    it { is_expected.to eql(:kafka) }
  end # describe .adapter

  describe ".new" do
    subject { command }

    it { is_expected.to be_kind_of ROM::Commands::Create }
  end # describe .new

  describe "#relation" do
    subject { command.relation }

    it { is_expected.to eql(relation) }
  end # describe #relation

  describe "#using" do
    subject { command.using(options) }

    let(:options) { { foo: :FOO, bar: :BAR } }

    it "returns a relation" do
      expect(subject).to be_kind_of described_class
    end

    it "updates the relation" do
      expect(relation).to receive(:using).with(options)
      expect(subject.relation).to eql(updated)
    end
  end # describe #using

  describe "#execute" do
    subject { command.execute(*tuples) }

    it "publishes tuples to the dataset" do
      expect(dataset).to receive(:publish).with(*tuples)
      subject
    end

    it "returns tuples" do
      expect(subject).to eql(tuples)
    end
  end # describe #execute

end # describe ROM::Kafka::Relation
