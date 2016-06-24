# -*- coding: utf-8 -*-
module SeedExpress
  class Supporters
    class << self
      def regist!
        define_to_h
        define_pluck
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
          def pluck
            if Array === args
              self.select(args).map { |v| args.map { |column| v.send(column) }}
            else
              self.select(args).map { |v| v.send(args) }
            end
          end
        end
      end
    end
  end
end
