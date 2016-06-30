# -*- coding: utf-8 -*-
module SeedExpress
  class RubyHash < Abstract
    def file_name
      @file_name ||= "#{@path}/#{table_name}.rb"
    end

    def in_records
      return @in_records if @in_records
      callbacks[:before_reading_data].call

      # Rubocop says, "The use of `eval` is a serious security risk",
      # but that is not a risk because `eval` execute only a specified file
      @in_records = eval File.read file_name

      callbacks[:after_reading_data].call(@in_records.size)
      @in_records
    end
  end
end
