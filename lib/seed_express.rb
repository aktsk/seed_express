# -*- coding: utf-8 -*-
require "seed_express/version"

module SeedExpress
  require 'csv'
  require 'pp'
  require 'tempfile'

  class Abstract
    attr_accessor :table_name
    attr_accessor :truncate_mode
    attr_accessor :nvl_mode
    attr_accessor :datetime_offset

    DEFAULT_NVL_CONVERSIONS = {
      :integer => 0,
      :string => '',
    }

    DEFAULT_CONVERSIONS = {
      :datetime => ->(seed_express, value) {  Time.zone.parse(value) + seed_express.datetime_offset },
    }

    COMMENT_COLUMN_CHARACTER = '#'

    def initialize(table_name, path, options)
      @table_name = table_name
      @path = path

      @table_to_klasses =
        if options[:table_to_klasses]
          options[:table_to_klasses]
        else
          {}
        end

      @filter_each_lines =
        if options[:filter_each_lines]
          options[:filter_each_lines]
        else
          nil
        end

      @filter_proc = options[:filter_proc]

      self.truncate_mode = options[:truncate_mode]
      self.nvl_mode = options[:nvl_mode]
      self.datetime_offset = options[:datetime_offset] || 0
    end

    def file_name
      @file_name ||= "#{@path}/#{table_name}.csv"
    end

    def klass
      return @klass if @klass
      @klass = @table_to_klasses[@table_name]
      unless @klass
        @klass = @table_name.to_s.classify.constantize
      end

      @klass
    end

    def seed_table
      return @seed_table if @seed_table
      @seed_table = SeedTable.where(table_name: klass.table_name).first
      @seed_table = SeedTable.create!(table_name: klass.table_name) unless @seed_table
      @seed_table
    end

    def table_digest
      return @table_digest if @table_digest
      @table_digest = Digest::SHA1.hexdigest(File.read(file_name))
    end

    def truncate_table
      klass.connection.execute("TRUNCATE TABLE #{klass.table_name};")
      SeedRecord.where(seed_table_id: seed_table.id).delete_all
    end

    def csv_values_with_header
      return @csv_values_with_header if @csv_values_with_header
      @csv_values_with_header = nil
      if @filter_each_lines
        Tempfile.open(table_name.to_s) do |tmp_f|
          File.open(file_name) do |f|
            f.each_line do |line|
              tmp_f.puts @filter_each_lines.call(line)
            end
          end

          tmp_f.flush
          @csv_values_with_header = ::CSV.read(tmp_f.path,
                                               {
                                                 :headers => false,
                                                 :converters => [],
                                                 :encoding => "UTF-8",
                                               })
        end
      else
        @csv_values_with_header = ::CSV.read(file_name,
                                             {
                                               :headers => false,
                                               :converters => [],
                                               :encoding => "UTF-8",
                                             })
      end

      @csv_values_with_header
    end

    def csv_values
      return @csv_values if @csv_values

      csv_rows = csv_values_with_header
      headers = csv_rows.shift.map(&:to_sym)
      auto_id = 1 unless headers.include?(:id)

      @csv_values = []
      csv_rows.map do |row|
        Hash[headers.zip(row)]
      end.each do |values|
        if auto_id
          values[:id] = auto_id
          auto_id += 1
        else
          values[:id] = values[:id].to_i
        end

        # Deletes comment columns
        values.delete_if do |k, v|
          k.to_s[0] == COMMENT_COLUMN_CHARACTER
        end

        if @filter_proc
          values = @filter_proc.call(values)
        end

        @csv_values << values if values
      end

      @csv_values
    end

    def duplicate_ids(values)
      hash = Hash.new(0)
      values.each do |value|
        hash[value[:id]] += 1
      end
      hash.delete_if do |k, v|
        v == 1
      end

      hash
    end

    def import_csv
      if truncate_mode
        # 必要ならば、テーブルを truncate
        truncate_table
      elsif seed_table.digest == table_digest
        # テーブルのダイジェスト値が同じ場合は処理をスキップする
        return :skipped
      end

      # 削除されるレコードを削除
      deleted_count = delete_missing_data

      # 新規登録対象と更新対象に分離
      inserting_records,
      inserting_digests,
      updating_records,
      updating_digests =
        take_out_each_types_of_data_to_upload


      # 新規登録するレコードを更新
      inserted_count = insert_records(inserting_records, inserting_digests)

      # 更新するレコードを更新
      updated_count, actual_updated_count = update_records(updating_records, updating_digests)

      # 不要な digest を削除
      delete_waste_seed_records

      # テーブルダイジェストを更新
      seed_table.update_attributes!(:digest => table_digest)


      return :done, inserted_count, updated_count, actual_updated_count, deleted_count
    end

    def convert_value(column, value)
      if value.nil?
        return defaults_on_db[column] if defaults_on_db.has_key?(column)
        return nvl(column, value)
      end
      conversion = DEFAULT_CONVERSIONS[columns[column].type]
      return value unless conversion
      conversion.call(self, value)
    end

    def defaults_on_db
      return @defaults_on_db if @defaults_on_db
      @defaults_on_db = {}
      klass.columns.each do |column|
        unless column.default.nil?
          @defaults_on_db[column.name.to_sym] = column.default
        end
      end
      @defaults_on_db
    end

    def nvl(column, value)
      return nil unless nvl_mode
      nvl_columns[column]
    end

    def nvl_columns
      return @nvl_columns if @nvl_columns
      @nvl_columns = {}
      klass.columns.each do |column|
        @nvl_columns[column.name.to_sym] = DEFAULT_NVL_CONVERSIONS[column.type]
      end

      @nvl_columns
    end

    def columns
      return @columns if @columns
      @columns = klass.columns.index_by { |column| column.name.to_sym }
      @columns.default_proc = lambda { |h, k| raise "#{klass.to_s}##{k} is not found" }
      @columns
    end

    def existing_ids
      return @existing_ids if @existing_ids

      @existing_ids = {}
      klass.select(:id).map(&:id).each do |id|
        @existing_ids[id] = true
      end

      @existing_ids
    end

    def delete_missing_data
      new_ids = csv_values.map { |value| value[:id] }
      delete_target_ids = existing_ids.keys - new_ids
      if delete_target_ids.present?
        klass.where(id: delete_target_ids).delete_all
      end

      delete_target_ids.size
    end

    def existing_digests
      return @existing_digests if @existing_digests
      @existing_digests = {}
      SeedRecord.where(seed_table_id: seed_table.id).map do |record|
        @existing_digests[record.record_id] = record.digest
      end

      @existing_digests
    end

    def take_out_each_types_of_data_to_upload
      inserting_records = []
      inserting_digests = {}
      updating_records = []
      updating_digests = {}

      duplicate_ids = duplicate_ids(csv_values)
      if duplicate_ids.present?
        raise "There are dupilcate ids. ({id=>num}: #{duplicate_ids.inspect})"
      end

      csv_values.each do |value|
        id = value[:id]
        digest  = Digest::SHA1.hexdigest(MessagePack.pack(value))


        if existing_ids[id]
          if existing_digests[id] != digest
            updating_records << value
            updating_digests[id] = digest
          end
        else
          inserting_records << value
          inserting_digests[id] = digest
        end
      end

      return inserting_records, inserting_digests, updating_records, updating_digests
    end

    def insert_records(records, digests)
      records_count = records.size
      block_size = 1000

      while(records.present?) do
        targets = records.slice!(0, block_size)
        ActiveRecord::Base.transaction do
          # マスタ本体をアップデート
          bulk_records = targets.map do |attributes|
            model = klass.new
            attributes.each_pair do |column, value|
              model[column] = convert_value(column, value)
            end
            model
          end
          klass.import(bulk_records)

          # SeedRecords をアップデート
          bulk_records = targets.map do |record|
            SeedRecord.new(seed_table_id: seed_table.id,
                           record_id:     record[:id],
                           digest:        digests[record[:id]])
          end
          SeedRecord.import(bulk_records)
        end
      end

      records_count
    end

    def update_records(records, digests)
      records_count = records.size
      actual_updating_count = 0
      block_size = 1000

      while(records.present?) do
        targets = records.slice!(0, block_size)
        record_ids = targets.map { |target| target[:id] }

        existing_records = klass.where(id: record_ids).index_by(&:id)
        existing_digests = SeedRecord.where(seed_table_id: seed_table.id,
                                            record_id: record_ids).index_by(&:record_id)

        ActiveRecord::Base.transaction do
          bulk_seed_records = []
          targets.each do |attributes|
            #
            # NOTE: ここでのダイジェスト値のチェックは不要
            #       ダイジェストのチェックはあくまで処理開始前のアップデート済みレコードの切り捨てに使用する
            #

            # マスタ本体をアップデート
            id = attributes[:id]
            model = existing_records[id]
            attributes.each_pair do |column, value|
              model[column] = convert_value(column, value)
            end
            if model.changed?
              actual_updating_count += 1
              model.save!
            end

            # SeedRecords をアップデート
            seed_record = existing_digests[id]
            if seed_record
              seed_record.digest = digests[id]
              seed_record.save!

            else
              bulk_seed_records << SeedRecord.new(seed_table_id: seed_table.id,
                                                  record_id: id,
                                                  digest: digests[id])
            end
          end
          SeedRecord.import(bulk_seed_records)
        end
      end

      return records_count, actual_updating_count
    end

    def delete_waste_seed_records
      master_record_ids = klass.all.map(&:id)
      seed_record_ids = SeedRecord.where(seed_table_id: seed_table.id).map(&:record_id)
      waste_record_ids = seed_record_ids - master_record_ids

      SeedRecord.where(seed_table_id: seed_table.id,
                       record_id: waste_record_ids).delete_all
    end
  end

  autoload :CSV, 'seed_express/csv'
end
