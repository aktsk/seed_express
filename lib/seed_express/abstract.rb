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
    attr_reader   :file_path


    def initialize(table_name, path, options)
      Supporters.regist!

      @table_name = table_name
      @file_path = path

      @filter_proc = options[:filter_proc]
      default_callback_proc = Proc.new { |*args| }
      default_callbacks = [:truncating, :disabling_digests,
                           :reading_data, :deleting,
                           :inserting, :inserting_a_part,
                           :updating, :updating_a_part,
                           :updating_digests, :updating_a_part_of_digests,
                           :inserting_digests, :inserting_a_part_of_digests,
                           :making_bulk_digest_records, :making_a_part_of_bulk_digest_records,
                          ].flat_map do |v|
        ["before_#{v}", "after_#{v}"].map(&:to_sym)
      end.map { |v| [v, default_callback_proc ] }.to_h
      @callbacks = default_callbacks.merge(options[:callbacks] || {})

      self.truncate_mode = options[:truncate_mode]
      self.force_update_mode = options[:force_update_mode]
      self.nvl_mode = options[:nvl_mode]
      self.datetime_offset = options[:datetime_offset] || 0
      self.parent_validation = options[:parent_validation]
    end

    def target_model
      unless v = self.class.table_to_klasses[@table_name]
        raise "#{@table_name} isn't able to convert to a class object"
      end
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
      seed_table.parts
    end
    memoize :parts

    def converters
      Converter.new(target_model,
                    :nvl_mode        => nvl_mode,
                    :datetime_offset => datetime_offset
                    )
    end
    memoize :converters

    def truncate_table
      callbacks[:before_truncating].call
      ActiveRecord::Base.transaction do
        seed_table.truncate_digests
      end

      target_model.connection.execute("TRUNCATE TABLE #{@table_name}")
      callbacks[:after_truncating].call
    end

    def disable_digests
      callbacks[:before_disabling_digests].call
      ActiveRecord::Base.transaction do
        seed_table.disable_record_digests
      end
      callbacks[:before_disabling_digests].call
    end

    def in_records
      raise "Please implements #in_records in each class"
    end

    def import
      beginning_time = Time.zone.now

      if truncate_mode
        truncate_table
      elsif force_update_mode
        disable_digests
      end

      digests = {}
      deleted_ids = []
      inserted_ids = []
      inserted_error = false
      updated_ids = []
      actual_updated_ids = []
      updated_error = false

      # パート毎に処理する
      parts_updated = false
      self.parts.each.with_index(1) do |part, i|
        next unless part.updated?
        out_of_part = SeedExpress::Part.new(part, converters, callbacks, i, parts.size).import
        digests.merge!(out_of_part[0])
        deleted_ids        += out_of_part[1]
        inserted_ids       += out_of_part[2]
        inserted_error     |= out_of_part[3]
        updated_ids        += out_of_part[4]
        actual_updated_ids += out_of_part[5]
        updated_error      |= out_of_part[6]
        parts_updated = true
      end

      return {:result => :skipped, :elapsed_time => Time.zone.now - beginning_time} unless parts_updated

      # 処理後の Validation
      after_seed_express_error =
        after_seed_express_validation(:inserted_ids       => inserted_ids,
                                      :updated_ids        => updated_ids,
                                      :actual_updated_ids => actual_updated_ids,
                                      :deleted_ids        => deleted_ids)

      # 処理後の Validation 予約(親テーブルを更新)
      update_parent_digest_to_validate(:inserted_ids       => inserted_ids,
                                       :updated_ids        => updated_ids,
                                       :actual_updated_ids => actual_updated_ids,
                                       :deleted_ids        => deleted_ids)

      has_an_error = inserted_error || updated_error || after_seed_express_error
      unless has_an_error
        ActiveRecord::Base.transaction do
          seed_table.seed_records.renew_digests!(self, inserted_ids, updated_ids, digests)
          self.parts.renew_digests!
        end
      end

      result = has_an_error ? :error : :result
      elapsed_time = Time.zone.now - beginning_time

      return {
        :result               => result,
        :inserted_count       => inserted_ids.size,
        :updated_count        => updated_ids.size,
        :actual_updated_count => actual_updated_ids.size,
        :deleted_count        => deleted_ids.size,
        :elapsed_time         => elapsed_time,
      }
    end

    def after_seed_express_validation(args)
      return false unless target_model.respond_to?(:after_seed_express_validation)

      errors, = target_model.after_seed_express_validation(args)
      error = false
      if errors.present?
        STDOUT.puts
        STDOUT.puts errors.pretty_inspect
        error = true
      end

      return error
    end

    class << self
      extend Memoist

      def table_to_klasses
        # Enables full of models
        Find.find("#{Rails.root}/app/models") { |f| require f if /\.rb$/ === f }

        table_to_klasses = ActiveRecord::Base.subclasses.
          select { |klass| klass.respond_to?(:table_name) }.
          map { |klass| [klass.table_name.to_sym, klass] }.to_h
      end
      memoize :table_to_klasses
    end

    private

    def update_parent_digest_to_validate(args)
      return unless self.parent_validation
      parent_table = self.parent_validation
      parent_id_column = (parent_table.to_s.singularize + "_id").to_sym

      parent_ids = target_model.unscoped.where(:id => args[:inserted_ids] + args[:updated_ids]).
        group(parent_id_column).pluck(parent_id_column)

      parent_table_model = self.class.table_to_klasses[parent_table]
      SeedTable.get_record(parent_table_model).disable_record_digests(parent_ids)
    end
  end
end
