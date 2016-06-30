class RemoveDigestFromSeedTables < ActiveRecord::Migration
  def change
    remove_column :seed_tables, :digest
  end
end
