context "#total_per_hour_in_past_week_per_market" do
    setup do
      Timecop.freeze(Time.parse("2011-11-11 00:00:00 EST"))
      @time = (1.day + 12.hours).ago.to_datetime
    end

    teardown do
      Timecop.return
    end

    should "return a total_by_category_per_hours report if there's total on a started object" do
      object = FactoryGirl.create(:object, begins: 2.hours.ago)
      report = object.total_by_category_per_hours.create!(category_id: 4444,
                                                      category_total_last_week: 12345.6789,
                                                      hourly_total_last_week: 123.45,
                                                      calculated_at: @time)
      calculations_method = object.total_per_hour_in_past_week_per_market.first
      assert_equal(report.category_id, calculations_method[:category_id])
      assert_equal(report.hourly_total_last_week.to_f, calculations_method[:total])
      assert_equal(report.calculated_at, calculations_method[:report_date].to_datetime)
    end

    should "returns nil for objects that have not started" do
      object = FactoryGirl.create(:object, begins: 2.hours.from_now)
      object.total_by_category_per_hours.create(category_id: 4444,
                                                     category_total_last_week: 0.0,
                                                     hourly_total_last_week: 123.0, #total would error elsewhere, but added here to test begins
                                                     calculated_at: @time)
      assert_equal nil, object.total_per_hour_in_past_week_per_market
    end
  end
