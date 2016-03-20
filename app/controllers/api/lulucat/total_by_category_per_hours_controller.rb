class Api::::LuluCat::TotalByCategoryPerHoursController < ActionController::Base
  include Api::ApiKeyAuthentication

  set_service_credentials_with :api_key

  connect_service :create, Services::LuluCat::CreateTotalByCategoryPerHour do
    service_input.total_category_payload = params[:lulucat_data]
  end
end