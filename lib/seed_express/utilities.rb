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

    def do_each_block!(array, block_size, callback_info)
      processed_size = 0
      array_size = array.size
      part_count = callback_info[:part_count]
      part_total = callback_info[:part_total]
      callback_name = callback_info[:callback_name]
      before_callback_name = "before_#{callback_name}".to_sym
      after_callback_name = "after_#{callback_name}".to_sym
      args_lambda =
        if !!(part_count || part_total)
          -> { [part_count, part_total, processed_size, array_size] }
        else
          -> { [processed_size, array_size] }
        end

      while(array.present?)
        callbacks[before_callback_name].call(*args_lambda.call)
        targets = array.slice!(0, block_size)
        yield(targets)
        processed_size += targets.size
        callbacks[after_callback_name].call(*args_lambda.call)
      end
    end
  end
end
