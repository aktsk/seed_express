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
        define_validation(model)
        define_not_null_without_default_column_names(model)
        define_not_null_without_default_columns(model)
        define_is_not_null_without_default_column(model)
      end

      private

      def define_validation(model)
        model.class_eval do
          validate :validation_which_not_null_without_default

          private

          def validation_which_not_null_without_default
            target_columns = self.class.not_null_without_default_column_names
            target_columns.each do |column|
              next unless self[column].nil?
              self.errors[column] << "must be set"
            end
          end
        end
      end

      def define_not_null_without_default_column_names(model)
        model.class_eval do
          class << self
            extend Memoist

            def not_null_without_default_column_names
              self.not_null_without_default_columns.map(&:name).map(&:to_sym)
            end
            memoize :not_null_without_default_column_names
          end
        end
      end

      def define_not_null_without_default_columns(model)
        model.class_eval do
          class << self
            def not_null_without_default_columns
              self.columns.select do |v|
                not_null_without_default_column?(v)
              end
            end
          end
        end
      end

      def define_is_not_null_without_default_column(model)
        model.class_eval do
          class << self
            def not_null_without_default_column?(column)
              !column.primary && !column.null && column.default.nil?
            end
          end
        end
      end
    end
  end
end
