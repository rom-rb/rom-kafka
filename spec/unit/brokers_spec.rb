# encoding: utf-8

describe ROM::Kafka::Brokers do

  let(:default_brokers) { described_class.new }
  let(:custom_brokers) do
    described_class.new "foo", "bar:9093", hosts: ["baz:9092"], port: 9094
  end

  describe ".new" do
    subject { default_brokers }

    it { is_expected.to be_frozen }
  end # describe .new

  describe "#to_a" do
    context "by default" do
      subject { default_brokers.to_a }

      it { is_expected.to eql ["localhost:9092"] }
    end

    context "customized" do
      subject { custom_brokers.to_a }

      it { is_expected.to eql ["foo:9094", "bar:9093", "baz:9092"] }
    end
  end # describe #to_a

  describe "#==" do
    subject { default_brokers == other }

    context "with the same brokers" do
      let(:other) { described_class.new }

      it { is_expected.to eql true }
    end

    context "with different brokers" do
      let(:other) { custom_brokers }

      it { is_expected.to eql false }
    end
  end # describe #==

end # describe ROM::Kafka::Brokers
