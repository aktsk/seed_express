# -*- coding: utf-8 -*-
module SeedExpress
  module Utilities
    def mix_results!(results, new_results)
      new_results.each_pair do |k, v|
        case v
        when Array
          results[k] = (results[k] ? results[k] : []) + v
        when Hash
          results[k] = (results[k] ? results[k] : {}).merge!(v)
        when TrueClass, FalseClass
          results[k] = results.has_key?(k) ? results[k] || v : v
        end
      end
    end

    def do_each_block!(array, block_size, callback_name)
      processed_size = 0
      array_size = array.size
      callback_before = "before_#{callback_name}".to_sym
      callback_after = "after_#{callback_name}".to_sym
      args_lambda =
        if self.respond_to?(:part_total)
          -> { [self.part_count, self.part_total, processed_size, array_size] }
        else
          -> { [processed_size, array_size] }
        end

      while(array.present?)
        callbacks[callback_before].call(*args_lambda.call)
        targets = array.slice!(0, block_size)
        yield(targets)
        processed_size += targets.size
        callbacks[callback_after].call(*args_lambda.call)
      end
    end
  end
end
