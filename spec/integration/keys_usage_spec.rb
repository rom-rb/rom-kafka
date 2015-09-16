# encoding: utf-8
require "shared/scholars_topic"

describe "Keys Usage" do

  include_context :scholars_topic

  it "works" do
    # Add messages into the partition 1, to be extracted from key "4"
    expect(insert.with(key: 4).call("Thomas", "Judah").to_a).to eql [
      { value: "Thomas", topic: "scholars", key: "4" },
      { value: "Judah",  topic: "scholars", key: "4" }
    ]

    # Add messages into the partition 2, to be extracted from key "5"
    expect(insert.with(key: 5).call("Maria", "Philip").to_a).to eql [
      { value: "Maria",  topic: "scholars", key: "5" },
      { value: "Philip", topic: "scholars", key: "5" }
    ]

    # Look at the data in the partition 1
    expect(scholars.from(1).offset(0).call.to_a).to eql [
      { value: "Thomas", topic: "scholars", key: "4", offset: 0 },
      { value: "Judah",  topic: "scholars", key: "4", offset: 1 }
    ]

    # Look at the data in the partition 2
    expect(scholars.from(2).offset(0).call.to_a).to eql [
      { value: "Maria",  topic: "scholars", key: "5", offset: 0 },
      { value: "Philip", topic: "scholars", key: "5", offset: 1 }
    ]
  end

end # describe Keys Usage
