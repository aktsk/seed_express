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

    def with_elapsed_time
      beginning_time = Time.zone.now
      results = yield
      results[:elapsed_time] = Time.zone.now - beginning_time
      results
    end

    def do_each_block(array, block_size, whole_callback_name, block_callback_name)
      whole_callback_before,
      whole_callback_after,
      block_callback_before,
      block_callback_after =
        callbacks_for_do_each_block(whole_callback_name, block_callback_name)

      processed_size = 0
      whole_callback_before.call(*args_for_callbacks(processed_size, array))
      array.each_slice(block_size) do |targets|
        block_callback_before.call(*args_for_callbacks(processed_size, array))
        yield(targets)
        processed_size += targets.size
        block_callback_after.call(*args_for_callbacks(processed_size, array))
      end
      whole_callback_before.call(*args_for_callbacks(processed_size, array))
    end

    def callbacks_for_do_each_block(whole_callback_name, block_callback_name)
      whole_callback_before = "before_#{whole_callback_name}".to_sym
      whole_callback_after = "after_#{whole_callback_name}".to_sym
      block_callback_before = "before_#{block_callback_name}".to_sym
      block_callback_after = "after_#{block_callback_name}".to_sym

      [
       callbacks[whole_callback_before],
       callbacks[whole_callback_after],
       callbacks[block_callback_before],
       callbacks[block_callback_after],
      ]
    end

    def args_for_callbacks(processed_size, array)
      if self.respond_to?(:part_total)
        [self.part_count, self.part_total, processed_size, array.size]
      else
        [processed_size, array.size]
      end
    end
  end
end
