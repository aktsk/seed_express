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

      # ID 重複を検出
      detect_duplicated_ids

      # 削除されるレコードを削除
      deleted_ids = delete_missing_data

      # 新規登録対象と更新対象に分離
      inserting_records, updating_records, digests =
        categorize_each_types_of_data_to_upload

      # 新規登録するレコードを更新
      inserted_ids, inserted_error = insert_records(inserting_records)

      # 更新するレコードを更新
      updated_ids, actual_updated_ids, updated_error = update_records(updating_records)

      # 不要な digest を削除
      delete_waste_seed_records

      return digests, deleted_ids, inserted_ids, inserted_error, updated_ids, actual_updated_ids, updated_error
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

    def detect_duplicated_ids
      duplicated_ids = Hash.new(0)
      seed_part.values.each do |value|
        duplicated_ids[value[:id]] += 1
      end
      duplicated_ids.delete_if do |k, v|
        v == 1
      end

      return duplicated_ids.blank?
      raise "There are dupilcated ids. ({id=>num}: #{duplicated_ids.inspect})"
    end

    def insert_records(records)
      error = false
      records_count = records.size
      callbacks[:before_inserting].call(records_count)

      existing_record_count = count_full_records
      inserted_ids = []
      while(records.present?) do
        callbacks[:before_inserting_a_part].call(part_count, part_total, inserted_ids.size, records_count)
        targets = records.slice!(0, BLOCK_SIZE)
        out_inserted_ids, out_error = insert_a_block_of_records(targets)
        inserted_ids += out_inserted_ids
        error |= out_error
        callbacks[:after_inserting_a_part].call(part_count, part_total, inserted_ids.size, records_count)
      end

      current_record_count = count_full_records
      if current_record_count != existing_record_count + records_count
        raise "Inserting error has been detected. Maybe it's caused by duplicated key on not ID column. Try truncate mode."
      end

      callbacks[:after_inserting].call(inserted_ids.size)
      return inserted_ids, error
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
      return inserted_ids, error
    end

    def update_records(records)
      error = false
      records_count = records.size
      callbacks[:before_updating].call(records_count)

      updated_ids = []
      actual_updated_ids = []
      while(records.present?) do
        callbacks[:before_updating_a_part].call(part_count, part_total, updated_ids.size, records_count)
        targets = records.slice!(0, BLOCK_SIZE)
        out_updated_ids, out_actual_updated_ids, out_error = update_a_block_of_records(targets)
        updated_ids += out_updated_ids
        actual_updated_ids += out_actual_updated_ids
        error |= out_error
        callbacks[:before_updating_a_part].call(part_count, part_total, updated_ids.size, records_count)
      end

      callbacks[:after_updating].call(updated_ids.size)
      return updated_ids, actual_updated_ids, error
    end

    def update_a_block_of_records(records)
      record_ids = records.map { |target| target[:id] }
      existing_records = target_model.unscoped.where(:id => record_ids).index_by(&:id)
      error = false
      updated_ids = []
      actual_updated_ids = []
      ActiveRecord::Base.transaction do
        records.each do |attributes|
          id = attributes[:id]
          model = existing_records[id]
          attributes.each_pair do |column, value|
            model[column] = converters.convert_value(column, value)
          end
          if model.changed?
            if model.valid?
              model.save!
              actual_updated_ids << id
            else
              show_each_validation_error(record)
              error = true
            end
          end
          updated_ids << id
        end
      end

      return updated_ids, actual_updated_ids, error
    end

    def delete_waste_seed_records
      seed_table = seed_part.seed_table
      range_of_ids = seed_part.record_id_from .. seed_part.record_id_to

      master_record_ids = target_model.unscoped.where(:id => range_of_ids).pluck(:id)

      seed_record_ids = SeedRecord.
        by_seed_table_id(seed_table.id).
        by_record_id(range_of_ids).
        pluck(:record_id)

      waste_record_ids = seed_record_ids - master_record_ids
      SeedRecord.
        by_seed_table_id(seed_table.id).
        by_record_id(waste_record_ids).delete_all
    end

    def get_errors(errors)
      ar_v = ActiveRecord::VERSION
      if ([ar_v::MAJOR, ar_v::MINOR] <=> [3, 2]) < 0
        # for older than ActiveRecord 3.2
        errors
      else
        # for equal or newer than ActiveRecord 3.2
        errors.messages
      end
    end

    def show_each_validation_error(record)
      STDOUT.puts
      STDOUT.puts "When id is #{record.id}: "
      STDOUT.print get_errors(record.errors).pretty_inspect
    end

    def count_full_records
      ActiveRecord::Base.transaction { target_model.unscoped.count }  # To read certainly from master server
    end
  end
end
