class CreateSeedRecords < ActiveRecord::Migration
  def self.up
    create_table :seed_records do |t|
      t.references :seed_table, null: false
      t.integer    :record_id,  null: false
      t.string     :digest,     null: false
      t.timestamps
    end

    add_index :seed_records, [:seed_table_id], name: :idx_seed_records_001
    add_index :seed_records, [:seed_table_id, :record_id], unique: true, name: :idx_seed_records_002
  end

  def self.down
    remove_table :seed_tables
    remove_index :idx_seed_records_001
    remove_index :idx_seed_records_002
  end
end
