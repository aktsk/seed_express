# -*- coding: utf-8 -*-
module SeedExpress
  class RubyHash < Abstract
    def file_name
      @file_name ||= "#{@path}/#{table_name}.hash"
    end

    def in_records
      return @in_records if @in_records
      callbacks[:before_reading_data].call

      # 静的チェックツールからセキュリティリスクだと言われるが、決められたファイルのみ評価する前提のため無視
      @in_records = eval File.read file_name

      callbacks[:after_reading_data].call(@in_records.size)
      @in_records
    end

  end
end
