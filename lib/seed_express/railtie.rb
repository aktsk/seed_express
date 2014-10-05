class Railtie < Rails::Railtie
  initializer "seed_express" do
    Rails.application.config.paths["db/migrate"] << "#{File.dirname(__FILE__)}/../../db/migrate"
    load "#{File.dirname(__FILE__)}/../../app/models/seed_record.rb"
    load "#{File.dirname(__FILE__)}/../../app/models/seed_table.rb"
  end
end
