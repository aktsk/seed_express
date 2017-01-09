class SeedTable < ActiveRecord::Base
  extend Memoist

  attr_accessor :target_model
  attr_accessor :reader

  has_many :seed_parts
  has_many :seed_records

  def disable_record_digests(record_ids = nil)
    query = self.seed_records
    query = query.where(:record_id => record_ids) if record_ids
    query.update_all(:digest => nil)

    if record_ids.blank?
      self.seed_parts.update_all(:digest => nil)
      return
    end

    record_ids = record_ids.dup
    deletions = {}
    self.seed_parts.each do |seed_part|
      range = seed_part.record_id_from .. seed_part.record_id_to
      record_ids.delete_if do |record_id|
        next false unless range.cover?(record_id)
        deletions[seed_part.id] = true
        true
      end
    end

    SeedPart.where(:id => deletions.keys).update_all(:digest => nil)
  end

  def parts
    files = SeedPart.part_files(self.reader) || SeedPart.files(self.reader)
    if files.blank?
      file_path = self.reader.file_path
      table_name = self.reader.table_name.to_s
      suffix = self.reader.class::FILE_SUFFIX

      part_file_path = "#{file_path}/**/#{table_name}.*-*.#{suffix}"
      file_path = "#{file_path}/**/#{table_name}.#{suffix}"
      raise "#{part_file_path} or #{file_path} do not exist."
    end

    parts = self.seed_parts.index_by { |v| v.record_id_from .. v.record_id_to }
    SeedExpress::Parts.new(self, files, parts)
  end
  memoize :parts

  def truncate_digests
    SeedPart.by_seed_table_id(self.id).delete_all
    SeedRecord.by_seed_table_id(self.id).delete_all
  end

  def schema_updated?
    self.schema_digest != target_model.schema_digest
  end

  class << self
    def get_record(target_model)
      table_name = target_model.table_name
      record =
        self.where(:table_name => table_name).first || self.create!(:table_name => table_name)

      record.target_model = target_model
      record
    end
  end
end
