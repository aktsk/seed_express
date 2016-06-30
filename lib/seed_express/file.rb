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
  end
end
