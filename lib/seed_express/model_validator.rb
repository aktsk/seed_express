module SeedExpress
  module ModelValidator
    class << self
      def included(klass)
        klass.extend ClassMethods
        klass.class_eval do
          validate :validation_for_not_null_without_default
          validate :validation_for_oversize_string
        end
      end
    end

    def validation_for_not_null_without_default
      target_columns = self.class.not_null_without_default_columns
      target_columns.each do |column|
        next unless self[column].nil?
        self.errors[column] << "must be set"
      end
    end

    def validation_for_oversize_string
      target_columns = self.class.string_columns
      target_columns.each_pair do |column, limit|
        v = self[column]
        next if v.nil?
        next if v.size <= limit
        self.errors[column] << "It is #{v.size} letters. It must be less or equal than #{limit} letters"
      end
    end

    module ClassMethods
      extend Memoist

      MAGIC_FIELD_NAMES = [:created_at, :created_on, :updated_at, :updated_on]

      def not_null_without_default_columns
        self.columns.select do |v|
          not_null_without_default_column?(v)
        end.map do |v|
          symbol = v.name.to_sym
          next if MAGIC_FIELD_NAMES.include?(symbol)
          symbol
        end.compact
      end
      memoize :not_null_without_default_columns

      def string_columns
        self.columns.select do |v|
          v.type == :string
        end.map do |v|
          [v.name.to_sym, v.limit]
        end.to_h
      end
      memoize :string_columns

      private

      def not_null_without_default_column?(column)
        !column.primary && !column.null && column.default.nil?
      end
    end
  end
end
