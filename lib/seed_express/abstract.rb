module SeedExpress
  class Abstract
    extend Memoist

    attr_accessor :table_name
    attr_accessor :truncate_mode
    attr_accessor :force_update_mode
    attr_accessor :nvl_mode
    attr_accessor :datetime_offset
    attr_accessor :callbacks
    attr_accessor :parent_validation
    attr_accessor :user_defined_conversions
    attr_reader   :file_path

    include SeedExpress::Utilities
    include SeedExpress::DigestManager

    def initialize(table_name, path, options)
      Supporters.regist!

      @table_name = table_name
      @file_path = path

      @filter_proc = options[:filter_proc]
      @callbacks = default_callbacks.merge(options[:callbacks] || {})

      self.truncate_mode = options[:truncate_mode]
      self.force_update_mode = options[:force_update_mode]
      self.nvl_mode = options[:nvl_mode]
      self.datetime_offset = options[:datetime_offset] || 0
      self.parent_validation = options[:parent_validation]
      self.user_defined_conversions = options[:user_defined_conversions] || {}
    end

    def target_model
      unless v = ModelClass.from_table(@table_name)
        raise "#{@table_name} isn't able to convert to a class object"
      end
      v.class_eval { include SeedExpress::ModelValidator }
      v
    end
    memoize :target_model

    def seed_table
      obj = SeedTable.get_record(target_model)
      obj.reader = self
      obj
    end
    memoize :seed_table

    def parts
      ActiveRecord::Base.transaction do
        seed_table.parts
      end
    end
    memoize :parts

    def converters
      Converter.new(target_model,
                    :nvl_mode                 => nvl_mode,
                    :datetime_offset          => datetime_offset,
                    :user_defined_conversions => user_defined_conversions,
                    )
    end
    memoize :converters

    def in_records
      raise "Please implements #in_records in each class"
    end

    def import
      with_elapsed_time do
        do_truncate_or_force_update

        r = {}
        # パート毎に処理する
        import_parts(r)
        next {:result => :skipped} unless r[:parts_updated]

        # 処理後の Callback 呼び出し
        call_later_seed_express(r)

        # 処理後の Validation 予約(親テーブルを更新)
        update_parent_digest_to_validate(r)

        # ダイジェストの更新
        renew_digests(r, has_an_error?(r))

        format_results(r)
      end
    end

    private

    def default_callbacks
      default_callback_proc = Proc.new do |*args|
        # Do nothing
      end

      [:truncating, :disabling_digests,
       :reading_data, :deleting,
       :inserting, :inserting_a_part,
       :updating, :updating_a_part,
       :updating_digests, :updating_a_part_of_digests,
       :inserting_digests, :inserting_a_part_of_digests,
       :making_bulk_digest_records, :making_a_part_of_bulk_digest_records,
      ].flat_map do |v|
        ["before_#{v}", "after_#{v}"].map(&:to_sym)
      end.map { |v| [v, default_callback_proc ] }.to_h
    end

    def import_parts(results)
      self.parts.each.with_index(1) do |part, i|
        next unless part.updated?
        out_results = SeedExpress::Part.new(part, converters, callbacks, i, parts.size).import
        mix_results!(results, out_results)
      end
    end

    def call_later_seed_express(args)
      # for backward compatibility
      callback_method = if target_model.respond_to?(:later_seed_express)
                          :later_seed_express
                        elsif target_model.respond_to?(:after_seed_express_validation)
                          :after_seed_express_validation
                        end
      return false unless callback_method

      callbacks[:before_later_seed_express_import].call
      errors, = target_model.send(callback_method, args)
      error = if errors.present?
                STDOUT.puts
                STDOUT.puts errors.pretty_inspect
                true
              else
                false
              end
      callbacks[:before_later_seed_express_import].call
      args[:later_seed_express_error] = error
    end

    def has_an_error?(results)
      results[:inserted_error] || results[:updated_error] || results[:later_seed_express_error]
    end

    def format_results(results)
      {
        :result               => has_an_error?(results) ? :error : :result,
        :inserted_count       => results[:inserted_ids].size,
        :updated_count        => results[:updated_ids].size,
        :actual_updated_count => results[:actual_updated_ids].size,
        :deleted_count        => results[:deleted_ids].size,
      }
    end
  end
end
