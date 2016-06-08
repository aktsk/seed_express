# -*- coding: utf-8 -*-
require 'rails'
require "seed_express/version"

module SeedExpress
  autoload :Railtie,  'seed_express/railtie'
  Railtie
  autoload :Abstract, 'seed_express/abstract'
  autoload :CSV,      'seed_express/csv'
  autoload :RubyHash, 'seed_express/rubyhash'
end
