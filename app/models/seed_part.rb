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
    target_model.unscoped.where(:id => (self.record_id_from .. self.record_id_to)).pluck(:id)
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
    records = ActiveRecord::Base.transaction do
      SeedRecord.where(:seed_table_id => seed_table.id,
                       :record_id => self.record_id_from .. self.record_id_to).
        pluck(:record_id, :digest)
    end

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

    def part_files(reader)
      files = get_part_files(reader)
      return nil if files.blank?
      sorted_files = sort_by_id_range(files)
      validate_ranges(sorted_files.keys)
      sorted_files
    end

    def files(reader)
      table_name = reader.table_name.to_s
      suffix = reader.class::FILE_SUFFIX
      patterns_for_glob = ["#{reader.file_path}/**/#{reader.table_name}.#{suffix}",
                           "#{reader.file_path}/**/#{reader.table_name}.*.#{suffix}"]
      files = Dir.glob(patterns_for_glob)
      return nil if files.blank?

      {
        WHOLE_ID_RANGE => PART_INFO_STRUCT.new(WHOLE_ID_RANGE, files.first)
      }
    end

    private

    def get_part_files(reader)
      table_name = reader.table_name.to_s
      suffix = reader.class::FILE_SUFFIX
      pattern_for_glob = "#{reader.file_path}/**/#{reader.table_name}.*-*.#{suffix}"
      pattern_for_regexp = %r!/#{table_name}\.([0-9]+)-([0-9]+)\.([^.]*\.)?#{suffix}$!i

      Dir.glob(pattern_for_glob).map do |file|
        each_part_file(file, pattern_for_regexp)
      end.compact.to_h
    end

    def each_part_file(file, pattern_for_regexp)
      return nil unless pattern_for_regexp === file

      id_from = $1.to_i
      id_to = $2.to_i
      raise "Incorrect partail file name(#{file})" if id_from > id_to

      id_range = id_from .. id_to
      [id_range, PART_INFO_STRUCT.new(id_range, file)]
    end

    def sort_by_id_range(part_files)
      part_files.keys.sort_by(&:min).map do |k|
        [k, part_files[k]]
      end.to_h
    end

    def validate_ranges(ranges)
      ranges.each_cons(2) do |small_range, big_range|
        if small_range.max >= big_range.min
          raise "id:(%d .. %d) and id:(%d .. %d) are overlapped" %
            [small_range.min, small_range.max, big_range.min, big_range.max]
        end
      end
    end
  end
end
