class BeNullableDigestOnSeedRecords < ActiveRecord::Migration
  def self.up
    change_column :seed_records, :digest, :string, :null => true
  end

  def self.down
    change_column :seed_records, :digest, :string, :null => false
  end
end
