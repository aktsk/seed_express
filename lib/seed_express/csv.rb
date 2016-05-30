# -*- coding: utf-8 -*-
module SeedExpress
  class CSV < Abstract
    require 'csv'
    require 'tempfile'

    COMMENT_INITIAL_CHARACTER = '#'

    def initialize(table_name, path, options)
      @filter_each_lines =
        if options[:filter_each_lines]
          options[:filter_each_lines]
        else
          nil
        end

      super
    end

    def file_name
      @file_name ||= "#{@path}/#{table_name}.csv"
    end

    def in_records
      return @in_records if @in_records

      callbacks[:before_reading_data].call
      csv_rows = csv_values_with_header
      headers = csv_rows.shift.map(&:to_sym)

      @in_records = []
      csv_rows.map do |row|
        Hash[headers.zip(row)]
      end.each do |values|
        values[:id] = values[:id].to_i

        # Deletes comment columns
        values.delete_if do |k, v|
          k.to_s[0] == COMMENT_INITIAL_CHARACTER
        end

        if @filter_proc
          values = @filter_proc.call(values)
        end

        @in_records << values if values
      end

      callbacks[:after_reading_data].call(@in_records.size)
      @in_records
    end

    private

    def csv_values_with_header
      return @csv_values_with_header if @csv_values_with_header
      @csv_values_with_header = nil

      Tempfile.open(table_name.to_s) do |tmp_f|
        File.open(file_name) do |f|
          tmp_f.puts f.gets   # Ignores header line
          f.each_line do |line|
            if @filter_each_lines
              line = @filter_each_lines.call(line)
            end

            next if line[0] == COMMENT_INITIAL_CHARACTER
            tmp_f.puts line
          end
        end

        tmp_f.flush
        @csv_values_with_header = ::CSV.read(tmp_f.path,
                                             {
                                               :headers => false,
                                               :converters => [],
                                               :encoding => "UTF-8",
                                             })
      end

      @csv_values_with_header
    end
  end
end
