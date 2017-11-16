# -*- coding: utf-8 -*-
module SeedExpress
  class Part
    autoload :Insert, 'seed_express/part/insert'
    autoload :Update, 'seed_express/part/update'
    autoload :Delete, 'seed_express/part/delete'

    BLOCK_SIZE = 1000

    extend Memoist

    attr_reader :seed_part
    attr_reader :callbacks
    attr_reader :target_model
    attr_reader :part_count
    attr_reader :part_total
    attr_reader :converters

    include SeedExpress::Utilities
    include SeedExpress::Part::Insert
    include SeedExpress::Part::Update
    include SeedExpress::Part::Delete

    def initialize(seed_part, converters, callbacks, part_count, part_total)
      @seed_part = seed_part
      @converters = converters
      @target_model = seed_part.target_model
      @callbacks = callbacks
      @part_count = part_count
      @part_total = part_total
    end

    def import
      return unless seed_part.updated?

      # 削除されるレコードを削除
      deleted_ids = delete_missing_data

      # 新規登録対象と更新対象に分離
      inserting_records, updating_records, digests =
        categorize_each_types_of_data_to_upload

      # 更新するレコードを更新
      update_results = update_records(updating_records)

      # 新規登録するレコードを更新
      insert_results = insert_records(inserting_records)

      # 不要な digest を削除
      delete_waste_seed_records

      # パート単位の処理後の処理
      call_later_a_part_of_seed_express(:inserted_records => inserting_records,
                                        :updated_records  => updating_records,
                                        :inserted_ids     => insert_results[:inserted_ids],
                                        :updated_ids      => update_results[:updated_ids],
                                        :deleted_ids      => deleted_ids,
                                        :digests          => digests)

      return {
        :digests            => digests,
        :deleted_ids        => deleted_ids,
        :inserted_ids       => insert_results[:inserted_ids],
        :inserted_error     => insert_results[:error],
        :updated_ids        => update_results[:updated_ids],
        :actual_updated_ids => update_results[:actual_updated_ids],
        :updated_error      => update_results[:error],
        :parts_updated      => true,
      }
    end

    private

    def categorize_each_types_of_data_to_upload
      inserting_records = []
      updating_records = []
      digests = {}

      existing_ids = seed_part.existing_ids_as_hash
      existing_digests = seed_part.existing_digests
      seed_part.values.each do |value|
        id = value[:id]
        digest  = Digest::SHA1.hexdigest(MessagePack.pack(value))

        if existing_ids[id]
          if existing_digests[id] != digest
            updating_records << value
            digests[id] = digest
          end
        else
          inserting_records << value
          digests[id] = digest
        end
      end

      return inserting_records, updating_records, digests
    end

    if  Gem::Version.new(ActiveRecord::VERSION::STRING) < Gem::Version.new("3.2.0")
      # for older than ActiveRecord 3.2
      def get_errors(errors)
        errors
      end
    else
      # for equal or newer than ActiveRecord 3.2
      def get_errors(errors)
        errors.messages
      end
    end

    def show_each_validation_error(record)
      STDOUT.puts
      STDOUT.puts "When id is #{record.id}: "
      STDOUT.print get_errors(record.errors).pretty_inspect
    end

    def target_columns
      target_model.column_names.map(&:to_sym).reject do |v|
        next true if v == :created_at || v == :updated_at
        false
      end.map { |v| [v, true] }.to_h
    end
    memoize :target_columns

    def set_value_into_model!(record, model)
      available_columns = target_columns
      record.each_pair do |column, value|
        next unless available_columns[column]
        model[column] = converters.convert_value(column, value)
      end
    end

    def call_later_a_part_of_seed_express(args)
      return false unless target_model.respond_to?(:later_a_part_of_seed_express)

      callbacks[:before_later_a_part_of_seed_express_import].call(part_count, part_total)
      errors, = target_model.later_a_part_of_seed_express(args)
      error = if errors.present?
                STDOUT.puts
                STDOUT.puts errors.pretty_inspect
                true
              else
                false
              end

      callbacks[:after_later_a_part_of_seed_express_import].call(part_count, part_total)
      error
    end
  end
end
