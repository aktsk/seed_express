module SeedExpress
  class Mysql
    class << self
      def strict_mode
        existing_sql_mode = self.sql_mode
        self.sql_mode = 'TRADITIONAL'
        yield
      ensure
        self.sql_mode = existing_sql_mode
      end

      def sql_mode
        ActiveRecord::Base.connection.select_value("SELECT @@session.sql_mode")
      end

      def sql_mode=(v)
        ActiveRecord::Base.connection.execute("SET session sql_mode = '#{v}'")
      end

      def set_null_validation(model)
        model.class_eval do
          validate :validation_which_not_null_without_default

          private

          def validation_which_not_null_without_default
            target_columns = self.class.not_null_without_default_column_name_symbols
            target_columns.each do |column|
              next unless self[column].nil?
              self.errors[column] << "must be set"
            end
          end

          class << self
            extend Memoist

            def not_null_without_default_column_name_symbols
              not_null_without_default_column_names.map(&:to_sym)
            end
            memoize :not_null_without_default_column_name_symbols

            private

            def not_null_without_default_column_names
              not_null_without_default_columns.map(&:name)
            end

            def not_null_without_default_columns
              self.columns.select do |v|
                not_null_without_default_column?(v)
              end
            end

            def not_null_without_default_column?(column)
              !column.primary && !column.null && column.default.nil?
            end
          end
        end
      end
    end
  end
end
