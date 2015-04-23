class CreateItems < ActiveRecord::Migration
  def self.up
    create_table :items do |t|
      t.string   :name,     :null => true
      t.datetime :start_at, :null => true
      t.integer  :counter,  :null => true
      t.float    :factor,   :null => true
      t.timestamps :null => true
    end
  end

  def self.down
    remove_table :items
  end
end
