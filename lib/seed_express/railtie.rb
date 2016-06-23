class Railtie < Rails::Railtie
  initializer "seed_express" do
    Rails.application.config.paths["db/migrate"] << "#{File.dirname(__FILE__)}/../../db/migrate"
    Dir.glob("#{File.dirname(__FILE__)}/../../app/models/*.rb").each do |file|
      load file
    end
  end
end
