# -*- coding: utf-8 -*-
module SeedExpress
  class CSV < Abstract
    require 'csv'
    require 'tempfile'

    COMMENT_INITIAL_CHARACTER = '#'
    FILE_SUFFIX = 'csv'

    def initialize(table_name, path, options)
      @filter_each_lines =
        if options[:filter_each_lines]
          options[:filter_each_lines]
        else
          nil
        end

      super
    end

    def read_values_from(data)
      callbacks[:before_reading_data].call
      csv_rows = csv_values_with_header_from(data)
      headers = csv_rows.shift.map(&:to_sym)

      whole_values = []
      csv_rows.map do |row|
        headers.zip(row).to_h
      end.each do |values|
        values[:id] = values[:id].to_i

        # Deletes comment columns
        values.delete_if do |k, v|
          k.to_s[0] == COMMENT_INITIAL_CHARACTER
        end

        if @filter_proc
          values = @filter_proc.call(values)
        end

        whole_values << values if values
      end

      callbacks[:after_reading_data].call(whole_values.size)
      whole_values
    end

    private

    def csv_values_with_header_from(data)
      values_with_header = nil
      Tempfile.open(table_name.to_s) do |tmp_f|
        data.each_line.with_index do |line, i|
          line.chomp!
          if i == 0
            tmp_f.puts line.chomp
            next
          end

          if @filter_each_lines
            line = @filter_each_lines.call(line)
          end

          next if line[0] == COMMENT_INITIAL_CHARACTER
          tmp_f.puts line
        end

        tmp_f.flush
        values_with_header = ::CSV.read(tmp_f.path,
                                        {
                                          :headers => false,
                                          :converters => [],
                                          :encoding => "UTF-8",
                                        })
      end

      values_with_header
    end

    def import_csv
      import
    end
  end
end
