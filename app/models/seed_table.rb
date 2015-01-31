class SeedTable < ActiveRecord::Base
  has_many :seed_records

  def self.get_record(object)
    case object
    when String
      table_name = object
    when Symbol
      table_name = object.to_sym
    when ActiveRecord::Base
      table_name = object.class.table_name
    when Class
      table_name = object.table_name
    end

    self.where(:table_name => table_name).first || self.create!(table_name: table_name)
  end

  def disable_record_cache(ids = nil)
    query = self.seed_records
    query = query.where(:record_id => ids) if ids
    query.update_all(:digest => nil)
  end
end
