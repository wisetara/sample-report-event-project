# BELOW ARE THE RELEVANT PARTS ON THE OBJECT MODEL FOR THIS REPORT

class Object

has_many :total_by_category_per_hours, -> { order(created_at: :desc) }

  def hourly_total_from_category
    results = []
    calculated_at_conversion = (1.day + 12.hours).ago
    reports = TotalByCategoryPerHour.where(object_id: self.id).where("calculated_at >= ?", calculated_at_conversion)
    reports.each do |report|
      results << { category_id: report.category_id, total: report.hourly_total_last_week.to_f, report_date: report.calculated_at }
    end
    results.empty? ? nil : results
  end
end