# encoding: utf-8
require "transproc/rspec"

describe ROM::Kafka::Functions, ".brokerize" do

  let(:arguments) { [:brokerize] }

  it_behaves_like :transforming_immutable_data do
    let(:input)  { [foo: 1] }
    let(:output) { { brokers: ["localhost:9092"], foo: 1 } }
  end

  let(:output) { { brokers: %w(127.0.0.1:9092 127.0.0.2:9093), foo: 1 } }

  it_behaves_like :transforming_immutable_data do
    let(:input) { ["127.0.0.1:9092", "127.0.0.2:9093", foo: 1] }
  end

  it_behaves_like :transforming_immutable_data do
    let(:input) { [hosts: ["127.0.0.1:9092", "127.0.0.2:9093"], foo: 1] }
  end

  it_behaves_like :transforming_immutable_data do
    let(:input) { [hosts: ["127.0.0.1", "127.0.0.2:9093"], port: 9092, foo: 1] }
  end

  it_behaves_like :transforming_immutable_data do
    let(:input) { ["127.0.0.1:9092", hosts: ["127.0.0.2"], port: 9093, foo: 1] }
  end

end # describe ROM::Kafka::Functions.brokerize
