class CreateSeedParts < ActiveRecord::Migration
  def change
    create_table :seed_parts do |t|
      t.references :seed_table,      :null => false
      t.integer    :record_id_from,  :null => false
      t.integer    :record_id_to,    :null => false
      t.string     :digest,          :null => true
      t.timestamps                   :null => true
    end

    add_index(:seed_parts,
              [:seed_table_id, :record_id_from, :record_id_to],
              :name => :idx_seed_parts_001,
              :unique => true)
  end
end
