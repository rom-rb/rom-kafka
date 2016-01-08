require "shared/scholars_topic"

describe "Basic Usage" do
  include_context :scholars_topic

  let(:add_scholars) { insert.with(key: 0) }

  it "works" do
    # Add messages into the 0 partition (see :add_scholars above)
    expect(add_scholars.call("Matthew", "Mark").to_a).to eql [
      { value: "Matthew", topic: "scholars", key: "0" },
      { value: "Mark",    topic: "scholars", key: "0" }
    ]

    # Fetching from the <default> 0 partition gives all messages
    expect(scholars.call.to_a).to eql [
      { value: "Matthew", topic: "scholars", key: "0", offset: 0 },
      { value: "Mark",    topic: "scholars", key: "0", offset: 1 }
    ]

    # Second call returns nothing because all messages were already fetched
    expect(scholars.call.to_a).to eql []

    # Add a couple of messages
    expect(add_scholars.call("Luke", "John").to_a).to eql [
      { value: "Luke", topic: "scholars", key: "0" },
      { value: "John", topic: "scholars", key: "0" }
    ]

    # And fetch them (now starting from the next offset from we stay before)
    expect(scholars.call.to_a).to eql [
      { value: "Luke", topic: "scholars", key: "0", offset: 2 },
      { value: "John", topic: "scholars", key: "0", offset: 3 }
    ]

    # Re-fetch all the messages from 0 offset
    expect(scholars.offset(0).call.to_a).to eql [
      { value: "Matthew", topic: "scholars", key: "0", offset: 0 },
      { value: "Mark",    topic: "scholars", key: "0", offset: 1 },
      { value: "Luke",    topic: "scholars", key: "0", offset: 2 },
      { value: "John",    topic: "scholars", key: "0", offset: 3 }
    ]

    # Re-fetch only limited subset of messages
    expect(scholars.offset(1).limit(2).call.to_a).to eql [
      { value: "Mark", topic: "scholars", key: "0", offset: 1 },
      { value: "Luke", topic: "scholars", key: "0", offset: 2 }
    ]

    # But actually this will move the offset not to 2 but to the end
    # (consumer fetched all the messages, but iterated via 2 only)
    # To start from the next offset, we should set it explicitly.
    expect(scholars.call.to_a).to eql []
  end
end
