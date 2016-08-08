module SeedExpress
  class ModelClass
    class << self
      extend Memoist
      def from_table(table)
        table_to_classes[table]
      end
      memoize :from_table

      private

      def table_to_classes
        # Enables full of models
        Find.find("#{Rails.root}/app/models") { |f| require f if /\.rb$/ === f }

        ActiveRecord::Base.subclasses.
          select { |klass| klass.respond_to?(:table_name) }.
          map { |klass| [klass.table_name.to_sym, klass] }.to_h
      end
      memoize :table_to_classes
    end
  end
end
