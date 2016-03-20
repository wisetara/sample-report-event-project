# excerpt from object_test.rb

context "total_from_category" do
    setup do
      Timecop.freeze(Time.parse("2011-11-11 00:00:00 EST"))
      @time = (1.day + 12.hours).ago.to_datetime
      @object = FactoryGirl.create(:object)
      @report = @object.total_by_category_per_hours.create(category_id: 4444,
                                                       category_total_last_week: 12345.6789,
                                                       hourly_total_last_week: 123.45,
                                                       calculated_at: @time)
    end

    teardown do
      Timecop.return
    end

    should "return nil if the last report is older than a day and a half" do
      @report.update_attribute(:calculated_at, (1.day + 12.hours + 1.minute).ago)
      assert_equal nil, @object.hourly_total_from_category
    end

    should "return the evaluated total rate if the last report is just over a day old" do
      @report.update_attribute(:calculated_at, (1.day + 1.second).ago)
      object_method = @object.hourly_total_from_category.first
      assert_equal(4444, object_method[:category_id])
      assert_equal(123.45, object_method[:total])
      assert_equal(@report.calculated_at, object_method[:report_date].to_datetime)
    end

    should "return the evaluated total rate if the last report is less than a day old" do
      @report.update_attribute(:created_at, 5.hours.ago)
      object_method = @object.hourly_total_from_category.first
      assert_equal(@report.category_id, object_method[:category_id])
      assert_equal(@report.hourly_total_last_week.to_f, object_method[:total])
      assert_equal(@report.calculated_at, object_method[:report_date].to_datetime)
    end
  end
