ROM::Kafka
==========

[![Gem Version](https://img.shields.io/gem/v/rom-kafka.svg?style=flat)][gem]
[![Build Status](https://img.shields.io/travis/rom-rb/rom-kafka/master.svg?style=flat)][travis]
[![Dependency Status](https://img.shields.io/gemnasium/rom-rb/rom-kafka.svg?style=flat)][gemnasium]
[![Code Climate](https://img.shields.io/codeclimate/github/rom-rb/rom-kafka.svg?style=flat)][codeclimate]
[![Coverage](https://img.shields.io/coveralls/rom-rb/rom-kafka.svg?style=flat)][coveralls]
[![Inline docs](http://inch-ci.org/github/rom-rb/rom-kafka.svg)][inch]

[Apache Kafka][kafka] support for [Ruby Object Mapper][rom] on top of [poseidon][poseidon] driver.

Installation
------------

Add this line to your application's Gemfile:

```ruby
# Gemfile
gem "rom-kafka"
```

Then execute:

```
bundle
```

Or add it manually:

```
gem install rom-kafka
```

Usage
-----

See the [corresponding Guide][guide] on [rom-rb.org][rom].

Before `v0.1.0` the gem is in alpha stage.

Compatibility
-------------

Compatible to [ROM][rom] 2.0+, [Apache Kafka][kafka] 0.8+.

Tested under [rubies supported by ROM][rubies].

Uses [RSpec][rspec] 3.0+ for testing and [hexx-suit][hexx-suit] dev/test tools collection.

Contributing
------------

* [Fork the project][github]
* Create your feature branch (`git checkout -b my-new-feature`)
* Add tests for it
* Run `rubocop` and `inch --pedantic` to ensure the style and inline docs are ok
* Run `rake mutant` or `rake exhort` to ensure 100% [mutant-proof][mutant] coverage
* Commit your changes (`git commit -am '[UPDATE] Add some feature'`)
* Push to the branch (`git push origin my-new-feature`)
* Create a new Pull Request

License
-------

See the [MIT LICENSE][license].

[codeclimate]: https://codeclimate.com/github/rom-rb/rom-kafka
[coveralls]: https://coveralls.io/r/rom-rb/rom-kafka
[gem]: https://rubygems.org/gems/rom-kafka
[gemnasium]: https://gemnasium.com/rom-rb/rom-kafka
[github]: https://github.com/rom-rb/rom
[guide]: http://rom-rb.org/guides/adapters/kafka
[hexx-suit]: https://github.com/nepalez/hexx-suit
[inch]: https://inch-ci.org/github/rom-rb/rom-kafka
[kafka]: http://kafka.apache.org
[license]: LICENSE
[mutant]: https://github.com/mbj/mutant
[poseidon]: https://github.com/bpot/poseidon
[rom]: http://rom-rb.org
[rspec]: http://rspec.org
[rubies]: .travis.yml
[travis]: https://travis-ci.org/rom-rb/rom-kafka