# encoding: utf-8

describe ROM::Kafka::Drivers::Base do

  let(:session) { described_class.new }

  describe "#new" do
    subject { session }

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

  describe "#publish" do
    subject { session.publish("Hi!", "Hello!") }

    it "raises NotImplementedError" do
      expect { subject }.to raise_error do |error|
        expect(error).to be_kind_of NotImplementedError
        expect(error.message)
          .to eql "The consumer cannot publish messages to a Kafka broker"
      end
    end
  end # describe #send

end # describe ROM::Kafka::Drivers::Base
