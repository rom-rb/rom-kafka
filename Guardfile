# encoding: utf-8

guard :rspec, cmd: "bundle exec rspec", all_on_start: true do

  watch(%r{^spec/.+_spec\.rb$})

  watch(%r{^lib/rom-kafka/(.+)\.rb}) do |m|
    "spec/unit/#{m[1]}_spec.rb"
  end

  watch("lib/rom-kafka.rb")    { "spec" }
  watch("spec/spec_helper.rb") { "spec" }

end # guard :rspec
