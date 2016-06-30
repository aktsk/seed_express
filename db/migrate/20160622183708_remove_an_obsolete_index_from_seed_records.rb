class RemoveAnObsoleteIndexFromSeedRecords < ActiveRecord::Migration
  def change
    remove_index :seed_records, :name => :idx_seed_records_001
  end
end
