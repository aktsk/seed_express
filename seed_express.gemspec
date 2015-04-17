# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'seed_express/version'

Gem::Specification.new do |spec|
  spec.name          = "seed_express"
  spec.version       = SeedExpress::VERSION
  spec.authors       = ["Toshiyuki Takaki"]
  spec.email         = ["ttakaki@aktsk.jp"]
  spec.description   = %q{高速 Seed データ登録をします}
  spec.summary       = %q{高速 Seed データ登録}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = Dir.glob('lib/**/*') +
    ['.gitignore', 'Gemfile', 'LICENSE.txt', 'README.md', 'Rakefile', 'seed_express.gemspec']
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"

  spec.add_dependency("rails", ">= 3.2.5")     # required by Railties
  spec.add_dependency("activerecord", ">= 3.0.0")
  spec.add_dependency("activerecord-import")
  spec.add_dependency("msgpack")
end
