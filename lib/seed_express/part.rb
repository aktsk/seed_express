# -*- coding: utf-8 -*-
module SeedExpress
  class Part
    BLOCK_SIZE = 1000

    extend Memoist

    attr_reader :seed_part
    attr_reader :callbacks
    attr_reader :target_model
    attr_reader :part_count
    attr_reader :part_total
    attr_reader :converters

    include SeedExpress::Utilities

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

      # 新規登録するレコードを更新
      insert_results = insert_records(inserting_records)

      # 更新するレコードを更新
      update_results = update_records(updating_records)

      # 不要な digest を削除
      delete_waste_seed_records

      return {
        :digests            => digests,
        :deleted_ids        => deleted_ids,
        :inserted_ids       => insert_results[:inserted_ids],
        :inserted_error     => insert_results[:error],
        :updated_ids        => update_results[:updated_ids],
        :actual_updated_ids => update_results[:actual_updated_ids],
        :updated_error      => update_results[:updated_error],
        :parts_updated      => true,
      }
    end

    private

    def delete_missing_data
      delete_target_ids = seed_part.existing_ids - seed_part.new_ids
      callbacks[:before_deleting].call(delete_target_ids.size)
      if delete_target_ids.present?
        target_model.unscoped.where(:id => delete_target_ids).delete_all
      end
      callbacks[:after_deleting].call(delete_target_ids.size)
      delete_target_ids
    end

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

    def insert_records(records)
      results = {:inserted_ids => []}
      do_each_block(records, BLOCK_SIZE, :inserting, :inserting_a_part) do |targets|
        mix_results!(results, insert_a_block_of_records(targets))
      end
      results
    end

    def insert_a_block_of_records(records)
      error = false
      inserted_ids = []
      bulk_records = records.map do |attributes|
        record = target_model.new
        attributes.each_pair do |column, value|
          record[column] = converters.convert_value(column, value)
        end

        if record.valid?
          inserted_ids << attributes[:id]
          record
        else
          show_each_validation_error(record)
          error = true
          nil
        end
      end.compact
      target_model.import(bulk_records)
      error |= detect_an_error_of_bulk_import(inserted_ids)

      return :inserted_ids => inserted_ids, :error => error
    end

    def detect_an_error_of_bulk_import(inserted_ids)
      actual_inserted_ids = ActiveRecord::Base.transaction do
        target_model.unscoped.where(:id => inserted_ids).pluck(:id)
      end

      lacking_ids = inserted_ids - actual_inserted_ids
      return false if lacking_ids.blank?

      ids_string = lacking_ids.join(', ')
      STDOUT.puts
      STDOUT.puts "Inserting error has been detected caused by ID: #{ids_string}. Maybe it's duplicated keys on not ID column. Fix it, and then run with truncate mode."

      true
    end

    def update_records(records)
      results = {:updated_ids => [], :actual_updated_ids => []}
      do_each_block(records, BLOCK_SIZE,
                    :updating, :updating_a_part) do |targets|
        mix_results!(results, update_a_block_of_records(targets))
      end
      results
    end

    def update_a_block_of_records(records)
      existing_records = existing_records_by_id(records)
      results = {:updated_ids => [], :actual_updated_ids =>[], :error => false}
      ActiveRecord::Base.transaction do
        records.each do |record|
          update_a_record!(record, existing_records, results)
        end
      end

      results
    end

    def existing_records_by_id(records_from_file)
      record_ids = records_from_file.map { |target| target[:id] }
      target_model.unscoped.where(:id => record_ids).index_by(&:id)
    end

    def update_a_record!(record, existing_records, results)
      id = record[:id]
      model = existing_records[id]
      record.each_pair do |column, value|
        model[column] = converters.convert_value(column, value)
      end
      if model.changed?
        if model.valid?
          model.save!
          results[:actual_updated_ids] << id
        else
          show_each_validation_error(model)
          results[:error] = true
        end
      end
      results[:updated_ids] << id
    end

    def delete_waste_seed_records
      range_of_ids = seed_part.record_id_from .. seed_part.record_id_to
      master_record_ids = target_model.unscoped.where(:id => range_of_ids).pluck(:id)
      SeedRecord.delete_waste_digests!(seed_part.seed_table, range_of_ids, master_record_ids)
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
      end
    end
  end
end
