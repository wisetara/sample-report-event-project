class TotalByCategoryPerHour < ActiveRecord::Base
  validates :object_id, :presence => true,
            :numericality => { :only_integer => true },
            :uniqueness => { :scope => [:category_id, :calculated_at] }
  validates :category_id, :presence => true,
            :numericality => { :only_integer => true }
  validates :category_total_last_week, :presence => true,
            :numericality => true
  validates :hourly_total_last_week, :presence => true,
            :numericality => { :greater_than_or_equal_to => 0.0 }
  validates :calculated_at, :presence => true
end