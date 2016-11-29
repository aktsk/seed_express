# -*- coding: utf-8 -*-
module SeedExpress
  class Converter
    extend Memoist

    DEFAULT_NVL_CONVERSIONS = {
      :integer => 0,
      :string => '',
    }

    DEFAULT_CONVERSIONS = {
      :datetime => ->(converter, value) do
        Time.zone.parse(value) + converter.datetime_offset
      end,
    }

    attr_reader :target_model
    attr_reader :nvl_mode
    attr_reader :datetime_offset
    attr_reader :user_defined_conversions

    def initialize(target_model, options)
      @target_model = target_model
      @nvl_mode = !!options[:nvl_mode]
      @datetime_offset = options[:datetime_offset]
      @user_defined_conversions = options[:user_defined_conversions]
    end

    def convert_value(column, value)
      if value.nil?
        return defaults_on_db[column] if defaults_on_db.has_key?(column)
        return nvl(column, value)
      end

      conversion = conversions[columns[column].type]
      return value unless conversion
      conversion.call(self, value)
    end

    def nvl(column, value)
      return nil unless nvl_mode
      nvl_columns[column]
    end

    def conversions
      DEFAULT_CONVERSIONS.merge(user_defined_conversions)
    end
    memoize :conversions

    private

    def defaults_on_db
      defaults = {}
      target_model.columns.each do |column|
        unless column.default.nil?
          defaults[column.name.to_sym] = column.default
        end
      end
      defaults
    end
    memoize :defaults_on_db

    def nvl_columns
      columns = {}
      target_model.columns.each do |column|
        columns[column.name.to_sym] = DEFAULT_NVL_CONVERSIONS[column.type]
      end
      columns
    end
    memoize :nvl_columns

    def columns
      columns = target_model.columns.index_by { |column| column.name.to_sym }
      columns.default_proc = lambda { |h, k| raise "#{target_model.to_s}##{k} is not found" }
      columns
    end
    memoize :columns
  end
end
