require 'test_helper'

class Api::LuluCat::TotalByCategoryPerHoursControllerTest < ActionController::TestCase
  tests Api::LuluCat::TotalByCategoryPerHoursController

  should "succeed when valid API is passed in" do
    @request.headers['X-SuperTara-API-Key'] = api_key_for(:api_client)
    post :create, lulucat_data: ""
    assert_response :success
  end

  should "fail when bad api key is passed in" do
    @request.headers['X-SuperTara-API-Key'] = "bad_api_key"
    post :create, lulucat_data: nil
    assert_response :unauthorized
  end

  should "handoff to service" do
    @request.headers['X-SuperTara-API-Key'] = api_key_for(:api_client)
    fake_object = Object.new
    fake_object.stubs(:data => "data", :status => :success)
    Services::LuluCat::CreateTotalByCategoryPerHour.any_instance.expects(:perform).once.returns(fake_object)
    post :create, lulucat_data: nil
    assert_response :success
  end
end
