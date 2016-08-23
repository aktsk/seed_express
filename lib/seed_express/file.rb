module SeedExpress
  class File
    extend Memoist

    attr_reader :id_range, :path, :reader

    def initialize(file_info, reader)
      @file_info = file_info
      return unless @file_info

      @id_range = file_info.id_range
      @path     = file_info.file_path
      @reader   = reader
    end

    def digest
      return unless @file_info
      Digest::SHA1.hexdigest(data)
    end
    memoize :digest

    def values
      rows = self.reader.read_values_from(data)
      validate_range_of_ids(rows)
      validate_duplicated_ids(rows)
      rows
    end
    memoize :values

    private

    def data
      ::File.read(self.path)
    end
    memoize :data

    def validate_range_of_ids(rows)
      allowed_range = @file_info.id_range
      rows.each do |row|
        next if allowed_range.cover?(row[:id])
        raise "#{@file_info.file_path} contains out of range id(#{row[:id]})"
      end
    end

    def validate_duplicated_ids(rows)
      duplicated_ids = Hash.new(0)
      rows.each do |value|
        duplicated_ids[value[:id]] += 1
      end
      duplicated_ids.delete_if do |k, v|
        v == 1
      end

      return if duplicated_ids.blank?
      raise "There are dupilcated ids. ({id=>num}: #{duplicated_ids.inspect})"
    end
  end
end
