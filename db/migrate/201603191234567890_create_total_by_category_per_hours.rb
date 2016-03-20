class CreateTotalByCategoryPerHours < ActiveRecord::Migration
  def change
    create_table :total_by_category_per_hours, :force => true do |t|
      t.integer  :object_id,                   :null => false
      t.integer  :category_id,                 :null => false
      t.decimal  :category_total_last_week,    :null => false, :precision => 10, :scale => 4
      t.decimal  :hourly_total_last_week,      :null => false, :precision => 10, :scale => 4
      t.datetime :calculated_at,               :null => false
      t.timestamps
    end

    add_index :total_by_category_per_hours, :object_id
    add_index :total_by_category_per_hours, :calculated_at
  end
end