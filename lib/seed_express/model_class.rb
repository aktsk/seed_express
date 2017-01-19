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

        get_real_classes = lambda do |models|
          results = []
          models.each do |model|
            if model.abstract_class && model.subclasses.size > 0
              results += get_real_classes.call(model.subclasses)
            else
              results << model
            end
          end
          results
        end

        not_abstract_classes = get_real_classes.call(ActiveRecord::Base.subclasses)

        not_abstract_classes.
          select { |klass| !klass.abstract_class && klass.respond_to?(:table_name) }.
          map { |klass| [klass.table_name.to_sym, klass] }.to_h
      end
      memoize :table_to_classes
    end
  end
end
