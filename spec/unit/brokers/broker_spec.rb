describe ROM::Kafka::Brokers::Broker do

  let(:broker) { described_class.new }

  describe "#host" do
    subject { broker.host }

    context "by default" do
      it { is_expected.to eql "localhost" }
    end

    context "when full :host is given" do
      let(:broker) { described_class.new host: :"https://some.path.com:9093" }

      it { is_expected.to eql "https://some.path.com" }
    end

    context "when localhost is given" do
      let(:broker) { described_class.new host: :"localhost:9093" }

      it { is_expected.to eql "localhost" }
    end
  end

  describe "#port" do
    subject { broker.port }

    context "by default" do
      it { is_expected.to eql 9092 }
    end

    context "when :host contains port" do
      let(:broker) { described_class.new host: :"https://some.path.com:9093" }

      it { is_expected.to eql 9093 }
    end

    context "when :port is set" do
      let(:broker) { described_class.new port: 9093 }

      it { is_expected.to eql 9093 }
    end

    context "when :host contains port and :port is set" do
      let(:broker) { described_class.new host: "localhost:9092", port: 9093 }

      it "prefers the host setting" do
        expect(subject).to eql 9092
      end
    end
  end

  describe "#to_s" do
    subject { described_class.new(host: :"127.0.0.1:9093").to_s }

    it { is_expected.to eql "127.0.0.1:9093" }
  end

  describe "#==" do
    subject { broker == other }

    context "with the same host and port" do
      let(:other) { described_class.new }

      it { is_expected.to eql true }
    end

    context "with another host" do
      let(:other) { described_class.new host: "127.0.0.1" }

      it { is_expected.to eql false }
    end

    context "with another port" do
      let(:other) { described_class.new port: 9093 }

      it { is_expected.to eql false }
    end
  end
end
