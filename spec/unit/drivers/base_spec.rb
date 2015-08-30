# encoding: utf-8

describe ROM::Kafka::Drivers::Base do

  let(:session) { described_class.new }

  describe "#new" do
    subject { session }

    it { is_expected.to be_kind_of ROM::Kafka::DSL::Attributes }
    it { is_expected.to be_kind_of Enumerable }
  end # describe #new

  describe "#each" do
    subject { session.each }

    it "raises NotImplementedError" do
      expect { subject }.to raise_error do |error|
        expect(error).to be_kind_of NotImplementedError
        expect(error.message)
          .to eql "The producer cannot fetch messages from a Kafka broker"
      end
    end
  end # describe #each

  describe "#send" do
    subject { session.send(value: "Hi", topic: "logs", key: "greetings") }

    it "raises NotImplementedError" do
      expect { subject }.to raise_error do |error|
        expect(error).to be_kind_of NotImplementedError
        expect(error.message)
          .to eql "The consumer cannot send messages to a Kafka broker"
      end
    end
  end # describe #send

  describe "#close" do
    subject { session.close }

    it { is_expected.to be_nil }
  end # describe #close

end # describe ROM::Kafka::Drivers::Base
