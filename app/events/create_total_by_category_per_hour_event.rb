class CreateTotalByCategoryPerHourEvent < ApplicationEvent

  def process
    payload = params[:payload]["tempfile"]
    if valid_payload?(payload)
      payload = payload.join if payload.is_a? Array
      parse_payload(payload)
    end
  end

  def object_ids
    @object_ids ||= []
  end

  private

  def valid_payload?(payload)
    !payload.nil? || !payload.empty?
  end

  def parse_payload(payload)
    CSV.parse(payload) do |(object_id, category_id, category_total_last_week, hourly_total_last_week, calculated_at)|
      begin
        next if object_id == "object_id" || object_id.to_i.zero? || !not_duplicate_objects_categories?(object_id, category_id)
        create_total_report(object_id, category_id, category_total_last_week, hourly_total_last_week, calculated_at)
        object_ids << object_id
      rescue Exception => error
        LS::Errors.report(error)
      end
    end
    object_ids_to_sync = LiveObject.pluck(:object_id) || object_ids
    # We are pushing the data over to an outside service immediately because there
    # really isn't a reason to wait, and there isn't an easy way to do that via
    # cron, because there are a number of factors on lulucat's side that cause the
    # execution time to vary (by a factor of hours).

    queue_action_event(:publish_objects_to_outside_service,
                       object_ids: object_ids_to_sync)
  end

  def create_total_report(object_id, category_id, category_total_last_week, hourly_total_last_week, calculated_at)
    object = Object.find_by_id(object_id)
    return if object.nil?

    if object && not_duplicate_objects_dates?(object.id, calculated_at)
      object.total_by_category_per_hours.create!(:category_id => category_id,
                                                 :category_total_last_week => category_total_last_week,
                                                 :hourly_total_last_week => hourly_total_last_week,
                                                 :calculated_at => calculated_at)
    end
  end

  def not_duplicate_objects_categories?(object_id, category_id)
    if TotalByCategoryPerHour.where(:object_id => object_id, :category_id => category_id).blank?
      true
    end
  end

  def not_duplicate_objects_dates?(object_id, calculated_at)
    if TotalByCategoryPerHour.where(:object_id => object_id, :calculated_at => calculated_at).blank?
      true
    end
  end
end
