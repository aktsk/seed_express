class AddSchemaDigestToSeedTables < ActiveRecord::Migration
  def change
    add_column :seed_tables, :schema_digest, :string, :null => true, :after => :table_name
  end
end
