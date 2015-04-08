# -*- coding: utf-8 -*-
require "rubygems"
require "bundler"

Bundler.setup()
require 'rspec/core'
require "pry"
require 'yaml'
require 'active_record'
require 'database_cleaner'

require "seed_express"

# load & set database configuration
database_config_yaml =  File.expand_path(File.dirname(__FILE__)) + "/config/database.yml"
database_config = YAML.load(File.open(database_config_yaml).read)
ActiveRecord::Base.establish_connection(database_config[Rails.env])

# migrate schema
[
 File.expand_path(File.join(File.dirname(__FILE__),  '/../db/migrate')),
 File.expand_path(File.join(File.dirname(__FILE__),  '/migrations')),
].each do |path|
  Dir.glob("#{path}/[0-9]*_*.rb").each do |file|
    version = file.sub(%r!^#{path}/!, '').sub(/_.*$/, '').to_i
    ActiveRecord::Migrator.run(:up, path, version)
  end
end

# load models for only spec
require "database_models"

# load application/library models
model_path = File.expand_path(File.dirname(__FILE__)) + "/../app/models"
Dir.glob("#{model_path}/*.rb").each do |model_file|
  require model_file
end

# This is settings for DatabaseCleaner
RSpec.configure do |config|
  config.before(:suite) do
    DatabaseCleaner.clean_with(:truncation)
  end

  config.before(:each) do
    DatabaseCleaner.strategy = :transaction
  end

  config.before(:each, :js => true) do
    DatabaseCleaner.strategy = :truncation
  end

  config.before(:each) do
    DatabaseCleaner.start
  end

  config.after(:each) do
    DatabaseCleaner.clean
  end
end
