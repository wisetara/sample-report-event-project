require 'csv'

module Services
  module LuluCat
    class CreateTotalByCategoryPerHour < Services::PerformService

      authorize  :api_client

      input_data :total_category_payload

      perform_steps :create

      def create
        queue_action_event(:create_total_by_category_per_hour,
                           :payload => total_category_payload,
                           :queue => "tara.medium")
        finish_with_result :success
      end
    end
  end
end