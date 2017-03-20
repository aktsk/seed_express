# -*- coding: utf-8 -*-
module SeedExpress
  class Supporters
    class << self
      def regist!
        define_to_h
        define_pluck
        define_schema_digest
      end

      private

      def define_to_h
        Enumerable.class_eval do
          return if self.instance_methods.include?(:to_h)
          def to_h
            self.inject({}) { |h, (k, v)| h[k] = v; h }
          end
        end
      end

      def define_pluck
        ActiveRecord::Relation.class_eval do
          return if self.instance_methods.include?(:pluck)
          def pluck(*args)
            if args.size > 1
              # Compatible for ActiveRecord 4.0(or later), multipluck 0.0.5(or later)
              pluck_for_columns(*args)
            else
              # Compatible for multipluck 0.0.4(or earlier)
              first = args.first
              if Enumerable === first
                pluck_for_columns(*first)
              else
                pluck_for_a_column(first)
              end
            end
          end

          private

          def pluck_for_columns(*args)
            self.select(args).map { |record| args.map { |column| record.send(column) } }
          end

          def pluck_for_a_column(argv)
            self.select(argv).map { |record| record.send(argv) }
          end
        end
      end

      def define_schema_digest
        ActiveRecord::Base.class_eval do
          class << self
            extend Memoist

            def schema_digest
              hash = self.columns.sort_by { |v| v.name }.map { |v| [v.name, v.sql_type] }.to_h
              Digest::SHA1.hexdigest(hash.to_msgpack)
            end
            memoize :schema_digest
          end
        end
      end
    end
  end
end
