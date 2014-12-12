class Abstract
  require 'csv'
  require 'pp'
  require 'msgpack'
  require 'tempfile'

  attr_accessor :table_name
  attr_accessor :truncate_mode
  attr_accessor :nvl_mode
  attr_accessor :datetime_offset
  attr_accessor :callbacks

  @@table_to_klasses = nil

  DEFAULT_NVL_CONVERSIONS = {
    :integer => 0,
    :string => '',
  }

  DEFAULT_CONVERSIONS = {
    :datetime => ->(seed_express, value) { Time.zone.parse(value) + seed_express.datetime_offset },
  }

  COMMENT_INITIAL_CHARACTER = '#'

  def initialize(table_name, path, options)
    @table_name = table_name
    @path = path

    @filter_each_lines =
      if options[:filter_each_lines]
        options[:filter_each_lines]
      else
        nil
      end

    @filter_proc = options[:filter_proc]
    default_callback_proc = Proc.new { |*args| }
    default_callbacks = [:truncating, :reading_data, :deleting,
                         :inserting, :inserting_a_part,
                         :updating, :updating_a_part,
                         :updating_digests, :updating_a_part_of_digests,
                         :inserting_digests, :inserting_a_part_of_digests,
                         :making_bulk_digest_records, :making_a_part_of_bulk_digest_records,
                        ].flat_map do |v|
      ["before_#{v}", "after_#{v}"].map(&:to_sym)
    end.flat_map { |v| [v, default_callback_proc ] }
    default_callbacks = Hash[*default_callbacks]
    @callbacks = default_callbacks.merge(options[:callbacks] || {})

    self.truncate_mode = options[:truncate_mode]
    self.nvl_mode = options[:nvl_mode]
    self.datetime_offset = options[:datetime_offset] || 0
  end

  def file_name
    @file_name ||= "#{@path}/#{table_name}.csv"
  end

  def klass
    return @klass if @klass
    @klass = self.class.table_to_klasses[@table_name]
    unless @klass
      raise "#{@table_name} isn't able to convert to a class object"
    end

    @klass
  end

  def seed_table
    return @seed_table if @seed_table
    @seed_table = SeedTable.where(table_name: @table_name).first
    @seed_table = SeedTable.create!(table_name: @table_name) unless @seed_table
    @seed_table
  end

  def table_digest
    return @table_digest if @table_digest
    @table_digest = Digest::SHA1.hexdigest(File.read(file_name))
  end

  def truncate_table
    callbacks[:before_truncating].call
    klass.connection.execute("TRUNCATE TABLE #{@table_name};")
    SeedRecord.where(seed_table_id: seed_table.id).delete_all
    callbacks[:after_truncating].call
  end

  def csv_values_with_header
    return @csv_values_with_header if @csv_values_with_header
    @csv_values_with_header = nil

    Tempfile.open(table_name.to_s) do |tmp_f|
      File.open(file_name) do |f|
        f.each_line do |line|
          if @filter_each_lines
            line = @filter_each_lines.call(line)
          end

          next if line[0] == COMMENT_INITIAL_CHARACTER
          tmp_f.puts line
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

    @csv_values_with_header
  end

  def csv_values
    return @csv_values if @csv_values

    callbacks[:before_reading_data].call
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
        k.to_s[0] == COMMENT_INITIAL_CHARACTER
      end

      if @filter_proc
        values = @filter_proc.call(values)
      end

      @csv_values << values if values
    end

    callbacks[:after_reading_data].call(@csv_values.size)
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
    deleted_ids = delete_missing_data

    # 新規登録対象と更新対象に分離
    inserting_records, updating_records, digests = take_out_each_types_of_data_to_upload

    # 新規登録するレコードを更新
    inserted_ids = insert_records(inserting_records)

    # 更新するレコードを更新
    updated_ids, actual_updated_ids = update_records(updating_records)

    # 不要な digest を削除
    delete_waste_seed_records

    # 処理後の Validation
    after_seed_express_validation(:inserted_ids       => inserted_ids,
                                  :updated_ids        => updated_ids,
                                  :actual_updated_ids => actual_updated_ids,
                                  :deleted_ids        => deleted_ids)

    # ダイジェスト値の更新
    update_digests(inserted_ids, updated_ids, digests)

    # テーブルダイジェストを更新
    seed_table.update_attributes!(:digest => table_digest)

    return :done, inserted_ids.size, updated_ids.size, actual_updated_ids.size, deleted_ids.size
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
      callbacks[:before_deleting].call(delete_target_ids.size)
      klass.where(id: delete_target_ids).delete_all
    end

    callbacks[:after_deleting].call(delete_target_ids.size)
    delete_target_ids
  end

  def existing_digests
    existing_digests = {}
    SeedRecord.where(seed_table_id: seed_table.id).map do |record|
      existing_digests[record.record_id] = record.digest
    end

    existing_digests
  end

  def take_out_each_types_of_data_to_upload
    inserting_records = []
    updating_records = []
    digests = {}

    duplicate_ids = duplicate_ids(csv_values)
    if duplicate_ids.present?
      raise "There are dupilcate ids. ({id=>num}: #{duplicate_ids.inspect})"
    end

    existing_digests = self.existing_digests
    csv_values.each do |value|
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
    records_count = records.size
    callbacks[:before_inserting].call(records_count)
    block_size = 1000

    inserted_ids = []
    while(records.present?) do
      callbacks[:before_inserting_a_part].call(inserted_ids.size, records_count)
      targets = records.slice!(0, block_size)
      ActiveRecord::Base.transaction do
        # マスタ本体をアップデート
        bulk_records = targets.map do |attributes|
          model = klass.new
          attributes.each_pair do |column, value|
            model[column] = convert_value(column, value)
          end

          unless model.valid?
            puts
            STDERR.puts "When id is #{model.id}: "
            STDERR.print model.errors.messages.pretty_inspect
            model.save!  # エラーを起こすことで強制終了する
          end

          inserted_ids << attributes[:id]
          model
        end
        klass.import(bulk_records)
      end
      callbacks[:after_inserting_a_part].call(inserted_ids.size, records_count)
    end

    callbacks[:after_inserting].call(inserted_ids.size)
    inserted_ids
  end

  def update_records(records)
    records_count = records.size
    callbacks[:before_updating].call(records_count)
    block_size = 1000

    updated_ids = []
    actual_updated_ids = []
    while(records.present?) do
      callbacks[:before_updating_a_part].call(updated_ids.size, records_count)
      targets = records.slice!(0, block_size)
      record_ids = targets.map { |target| target[:id] }

      existing_records = klass.where(id: record_ids).index_by(&:id)
      ActiveRecord::Base.transaction do
        bulk_seed_records = []
        targets.each do |attributes|
          id = attributes[:id]
          model = existing_records[id]
          attributes.each_pair do |column, value|
            model[column] = convert_value(column, value)
          end
          if model.changed?
            unless model.valid?
              puts
              STDERR.puts "When id is #{model.id}: "
              STDERR.print model.errors.messages.pretty_inspect
            end

            model.save!  # エラーがある場合は、エラーを起こすことで強制終了する
            actual_updated_ids << id
          end
          updated_ids << id
        end
        SeedRecord.import(bulk_seed_records)
      end
      callbacks[:before_updating_a_part].call(updated_ids.size, records_count)
    end

    callbacks[:after_updating].call(updated_ids.size)
    return updated_ids, actual_updated_ids
  end

  def delete_waste_seed_records
    master_record_ids = klass.all.map(&:id)
    seed_record_ids = SeedRecord.where(seed_table_id: seed_table.id).map(&:record_id)
    waste_record_ids = seed_record_ids - master_record_ids

    SeedRecord.where(seed_table_id: seed_table.id,
                     record_id: waste_record_ids).delete_all
  end

  def update_digests(inserted_ids, updated_ids, digests)
    tmp_updated_ids = updated_ids.dup
    block_size = 1000
    bulk_records = []
    existing_digests = SeedRecord.where(seed_table_id: seed_table.id,
                                        record_id: updated_ids).index_by(&:record_id)
    counter = 0
    callbacks[:before_updating_digests].call(counter, updated_ids.size)
    while tmp_updated_ids.present?
      callbacks[:before_updating_a_part_of_digests].call(counter, updated_ids.size)
      targets = tmp_updated_ids.slice!(0, block_size)
      targets.each do |id|
        seed_record = existing_digests[id]
        if seed_record
          seed_record.digest = digests[id]
          seed_record.save!
        else
          bulk_records << SeedRecord.new(seed_table_id: seed_table.id,
                                         record_id:     id,
                                         digest:        digests[id])
        end
      end
      counter += targets.size
      callbacks[:after_updating_a_part_of_digests].call(counter, updated_ids.size)
    end
    callbacks[:after_updating_digests].call(counter, updated_ids.size)

    counter = 0
    callbacks[:before_making_bulk_digest_records].call(0, inserted_ids.size)
    inserted_ids.each do |id|
      if counter % block_size == 0
        callbacks[:before_making_a_part_of_bulk_digest_records].call(counter, inserted_ids.size)
      end

      bulk_records << SeedRecord.new(seed_table_id: seed_table.id,
                                     record_id:     id,
                                     digest:        digests[id])
      counter += 1
      if counter % block_size == 0
        callbacks[:after_making_a_part_of_bulk_digest_records].call(counter, inserted_ids.size)
      end
    end
    callbacks[:after_making_bulk_digest_records].call(inserted_ids.size, inserted_ids.size)

    bulk_size = bulk_records.size
    counter = 0
    callbacks[:before_inserting_digests].call(counter, bulk_size)
    while bulk_records.present?
      callbacks[:before_inserting_a_part_of_digests].call(counter, bulk_size)
      targets = bulk_records.slice!(0, block_size)
      SeedRecord.import(targets)
      counter += targets.size
      callbacks[:after_inserting_a_part_of_digests].call(counter, bulk_size)
    end
    callbacks[:after_inserting_digests].call(counter, bulk_size)
  end

  def after_seed_express_validation(args)
    return unless klass.respond_to?(:after_seed_express_validation)
    errors = klass.after_seed_express_validation(args)
    return if errors.blank?

    STDOUT.puts
    STDOUT.puts errors.pretty_inspect
    raise ActiveRecord::StatementInvalid
  end

  def self.table_to_klasses
    return @@table_to_klasses if @@table_to_klasses

    # Enables full of models
    Find.find("#{Rails.root}/app/models") { |f| require f if /\.rb$/ === f }

    table_to_klasses = ActiveRecord::Base.subclasses.
      select { |klass| klass.respond_to?(:table_name) }.
      flat_map { |klass| [klass.table_name.to_sym, klass] }
    @@table_to_klasses = Hash[*table_to_klasses]
  end
end
