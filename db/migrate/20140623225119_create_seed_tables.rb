class CreateSeedTables < ActiveRecord::Migration
  def change
    create_table :seed_tables do |t|
      t.string :table_name, :null => false
      t.string :digest,     :null => true
      t.timestamps :null => true
    end

    add_index :seed_tables, [:table_name], :unique => true, :name => :idx_seed_tables_001
  end
end
