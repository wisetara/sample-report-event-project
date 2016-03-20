require 'test_helper'
require 'services_test_helper'

class Services::LuluCat::CreateTotalByCategoryPerHourTest < Test::Unit::TestCase

  context "total_by_category_per_hour_service" do
    should "queue an event to publish_objects_to_outside_service" do
      Services::LuluCat::CreateTotalByCategoryPerHour.any_instance.expects(:queue_action_event).
        with(:create_total_by_category_per_hour, {:payload => 'test_string', :queue => 'tara.medium'})
      response = perform_service(:total_category_payload => 'test_string')
      assert response.success?
    end

    should "not queue an event when no parameters are passed" do
      assert_raise(Services::PerformService::MissingInputData) do
        response = perform_service
      end
      Services::LuluCat::CreateTotalByCategoryPerHour.any_instance.expects(:queue_action_event).never
    end

    should "not queue an event when nil parameters are passed" do
      service = Services::LuluCat::CreateTotalByCategoryPerHour.new(:total_payload => nil)
      assert_nothing_raised do
        response = perform_service(:total_category_payload => nil)
      end
      Services::LuluCat::CreateTotalByCategoryPerHour.any_instance.expects(:queue_action_event).never
    end
  end

  def perform_service(args = {})
    args.merge!({
      :credentials  => credentials,
      :dependencies => { :confirm_authorization_service => success_service }
    })
    Services::LuluCat::CreateTotalByCategoryPerHour.new(args).perform
  end
end
