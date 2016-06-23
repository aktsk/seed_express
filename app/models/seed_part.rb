class SeedPart < ActiveRecord::Base
  extend Memoist

  belongs_to :seed_table
  scope :by_seed_table_id, ->(seed_table_id) { where(:seed_table_id => seed_table_id) }

  attr_accessor :file, :target_model

  WHOLE_ID_RANGE = (1 .. (2 ** 31 - 1))
  PART_INFO_STRUCT = Struct.new(:id_range, :file_path, :updated, :digest, :values)

  def updated?
    !stable?
  end
  memoize :updated?

  def stable?
    self.file.digest == self.digest
  end
  memoize :stable?

  def values
    self.file.values
  end
  memoize :values

  def new_ids
    self.values.map { |v| v[:id] }
  end
  memoize :new_ids

  def existing_ids
    target_model.where(:id => (self.record_id_from .. self.record_id_to)).pluck(:id)
  end
  memoize :existing_ids

  def existing_ids_as_hash
    hash = {}
    existing_ids.each do |v|
      hash[v] = true
    end
    hash
  end
  memoize :existing_ids_as_hash

  def existing_digests
    digests = {}
    records =
      SeedRecord.where(:seed_table_id => seed_table.id,
                       :record_id => self.record_id_from .. self.record_id_to).
      pluck([:record_id, :digest])

    records.each do |record_id, digest|
      digests[record_id] = digest
    end

    digests
  end
  memoize :existing_digests

  def update_digest!
    self.digest = self.file.digest
    self.save!
  end

  private

  class << self
    def setup(seed_table, id_range, file_info, db_info)
      record = if db_info
                 db_info
               else
                 self.new(:seed_table_id  => seed_table.id,
                          :record_id_from => id_range.min,
                          :record_id_to   => id_range.max)
               end

      record.file = SeedExpress::File.new(file_info, seed_table.reader)
      record.target_model = seed_table.target_model
      record
    end

    def part_files(pattern)
      part_files = {}
      Dir.glob(pattern).each do |file|
        next unless %r!\.([0-9]+)-([0-9]+)\.csv$!i === file

        id_from = $1.to_i
        id_to = $2.to_i
        if id_from > id_to
          raise "Incorrect partail file name(#{file})"
        end

        id_range = id_from .. id_to
        part_files[id_range] = PART_INFO_STRUCT.new(id_range, file)
      end

      part_files.present? ? part_files : nil
    end

    def files(pattern)
      return nil unless File.exists?(pattern)

      {
        WHOLE_ID_RANGE => PART_INFO_STRUCT.new(WHOLE_ID_RANGE, pattern)
      }
    end
  end
end
