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
    end
  end
end
