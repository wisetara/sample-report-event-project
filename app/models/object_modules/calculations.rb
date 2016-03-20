# THIS IS AN SECTION THAT WOULD BE INCLUDED IN A LARGER MODULE

module ObjectModules
  module Calculations

    HOURS_IN_A_WEEK = 24 * 7

    def self.included(base)
      base.extend ClassMethods
    end

    module ClassMethods

    def total_per_market_and_hour_in_past_week
      @total_per_market_and_hour_in_past_week ||= begin
        multiplier = [hours_since_object_started, HOURS_IN_A_WEEK].min
        divisor = category_ids.size * multiplier
        divisor == 0 ? 0.0 : total_from_past_week / divisor
      end
    end
  end
end
