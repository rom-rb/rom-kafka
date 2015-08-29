# encoding: utf-8

describe ROM::Kafka::DSL::Attributes do

  let(:klass)  { Class.new }
  let(:object) { klass.new }

  before { klass.send :include, described_class }

  describe ".attributes" do
    subject { klass.attributes }

    it { is_expected.to eql({}) }
  end # describe .attributes

  describe ".attribute" do
    context "without default value" do
      subject { klass.attribute :foo }

      it "registers the attribute" do
        expect { subject }
          .to change { klass.attributes }
          .from({})
          .to(foo: nil)
      end

      it "adds instance attribute" do
        expect { subject }
          .to change { klass.instance_methods }
          .by [:foo]
        expect(object.foo).to be_nil
      end
    end

    context "with default value" do
      subject { klass.attribute :foo, default: :FOO }

      it "registers the attribute" do
        expect { subject }
          .to change { klass.attributes }
          .from({})
          .to(foo: :FOO)
      end

      it "adds instance attribute" do
        expect { subject }
          .to change { klass.instance_methods }
          .by [:foo]
        expect(object.foo).to eql :FOO
      end
    end
  end # describe .attribute

  describe "#attributes" do
    subject { object.attributes }

    before { klass.attribute :foo, default: :FOO }

    it "returns default attributes" do
      expect(subject).to eql klass.attributes
    end
  end # describe #attributes

  describe "#slice" do
    subject { object.slice :bar, :baz, :qux }

    before { klass.attribute :foo, default: :FOO }
    before { klass.attribute :bar, default: :BAR }
    before { klass.attribute :baz, default: :BAZ }

    it "returns subhash of attributes" do
      expect(subject).to eql(bar: :BAR, baz: :BAZ)
    end
  end

  describe ".new" do
    subject { klass.new(foo: :QUX, baz: :BAZ) }

    before { klass.attribute :foo, default: :FOO }
    before { klass.attribute :bar, default: :BAR }

    it "sets declared attributes only" do
      expect(subject.attributes).to eql(foo: :QUX, bar: :BAR)
      expect(subject.foo).to eql(:QUX)
      expect(subject.bar).to eql(:BAR)
    end
  end # describe #initialize

end # describe ROM::Kafka::DSL::Attributes
