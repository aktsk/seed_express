module SeedExpress
  class File
    extend Memoist

    attr_reader :id_range, :path, :digest, :values, :reader

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
      self.reader.read_values_from(data)
    end
    memoize :values

    private

    def data
      ::File.read(self.path)
    end
    memoize :data
  end
end
