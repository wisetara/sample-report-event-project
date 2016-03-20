require "test_helper"

class CreateTotalByCategoryPerHourEventTest < ActiveSupport::TestCase

  context "CreateTotalByCategoryPerHourEvent" do
    context 'when processing succeeds' do
      setup do
        Timecop.freeze(Time.parse("2015-10-07 12:00:00 EDT"))
        @object_1 = FactoryGirl.create(:object)
        @object_2 = FactoryGirl.create(:object)
        @object_3 = FactoryGirl.create(:object)
        uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151007214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
          #{@object_1.id},4444,5000.01,1235.0,2015-10-7 12:00:00\n#{@object_2.id},1,5600,423,2015-10-7 12:00:00\n#{@object_3.id},1,3845.65,789.48,2015-10-7 12:00:00"]}
        @event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })
      end

      teardown do
        Timecop.return
      end

      should 'parse_payload' do
        CreateTotalByCategoryPerHourEvent.any_instance.expects(:parse_payload)
        @event.process
      end

      should 'queue action event' do
        CreateTotalByCategoryPerHourEvent.any_instance.expects(:queue_action_event)
          .with(:publish_objects_to_outside_service, object_ids: [])
        @event.process
        object_method = @object_1.hourly_total_from_category.first
        assert_equal(4444, object_method[:category_id])
        assert_equal(1235.0, object_method[:total])
        assert_equal(Time.parse('Wed, 07 Oct 2015 12:00:00 EDT -04:00'), object_method[:report_date].to_datetime)
      end

      should 'create_total_report' do
        assert_equal 0, @object_1.total_by_category_per_hours.count
        CreateTotalByCategoryPerHourEvent.any_instance.expects(:create_total_report).at_least_once
        @event.process
      end

      should 'sends the data into zee database' do
        uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_7_days,calculated_at\n",
          "#{@object_1.id},36,12.0,3.3,2015-10-28 12:00:00\n"]}
        event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })
        event.process

        result = @object_1.total_by_category_per_hours.last

        assert_not_nil(@object_1.total_by_category_per_hours.last)

        assert_equal(@object_1.id, result.object_id)
        assert_equal(36, result.category_id)
        assert_equal(12.0, result.category_total_last_week)
        assert_equal(3.3, result.hourly_total_last_week)
        assert_equal(DateTime.parse('2015-10-28 12:00:00 -0400'), result.calculated_at)
      end

      should 'not create duplicate reports' do
        uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
          #{@object_1.id},4444,2000.01,123.0,2015-10-06 12:00:00"]}
        event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

        assert_difference("TotalByCategoryPerHour.count", 1) do
          event.process
        end

        assert_no_difference("TotalByCategoryPerHour.count") do
          event.process
        end
      end

      should 'return some object_ids' do
        CreateTotalByCategoryPerHourEvent.any_instance.expects(:object_ids).at_least_once
        @event.process
      end

      should 'read some things after event processing' do
        @event.process
        object_method_1 = @object_1.hourly_total_from_category.first
        object_method_2 = @object_2.hourly_total_from_category.first
        assert_equal(4444, object_method_1[:category_id])
        assert_equal(1235.0, object_method_1[:total])
        assert_equal(423.0, object_method_2[:total])
      end

      should 'run a report when the database is empty for a particular object and date combo' do
        uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
          #{@object_1.id},4444,2000.01,123.0,2015-10-28 12:00:00"]}
        event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })
        CreateTotalByCategoryPerHourEvent.any_instance.expects(:not_duplicate_objects_dates?).returns(true)
        event.process
        object_method_1 = @object_1.hourly_total_from_category.first
        assert_equal(@object_1.total_by_category_per_hours.first.hourly_total_last_week, object_method_1[:total])
      end

      should 'allow backfill of data when no duplicate exists' do
        uploaded_file_1 = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
          #{@object_1.id},4444,2000.01,123.0,2015-10-03 12:00:00"]}
        uploaded_file_2 = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
          #{@object_2.id},4444,2000.01,123.0,2015-10-05 12:00:00"]}
        uploaded_file_3 = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
          #{@object_2.id},43,2000.01,123.0,2015-10-06 12:00:00"]}
        uploaded_file_4 = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
          #{@object_3.id},4444,2000.01,123.0,2015-10-05 12:00:00"]}
        event_1 = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file_1 })
        event_2 = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file_2 })
        event_3 = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file_2 })
        event_4 = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file_3 })
        event_5 = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file_4 })

        assert_difference("TotalByCategoryPerHour.count", 4) do
          event_1.process
          event_2.process
          event_3.process # event_3 attempts to run a duplicate report
          event_4.process # event_4 should process the same object_id on a different date
          event_5.process
        end
      end
    end
  end

  context '#not_duplicate_objects_dates?' do
    should 'when a duplicate object_id and date combo report is found' do
      object_1 = FactoryGirl.create(:object)
      existing_report = TotalByCategoryPerHour.create!(:object_id => object_1.id, :category_id => 4444, :category_total_last_week => 4567.09, :hourly_total_last_week => 123.67, :calculated_at => '2015-10-28 12:00:00')
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        #{object_1.id},15,2030.01,1234.0,2015-10-28 12:00:00"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      refute(event.send(:not_duplicate_objects_dates?, object_1.id, existing_report.calculated_at))
    end
  end

  context 'no database insertion' do
    should 'will notify ls-errors if there is an error' do
      Timecop.freeze(Time.parse("2015-10-07 12:00:00 EDT"))
      object_1 = FactoryGirl.create(:object)
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        #{object_1.id},15,2015.01,1245.0,2015-10-28 12:00:00"]}
      event_1 = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })
      CreateTotalByCategoryPerHourEvent.any_instance.expects(:create_total_report).raises(ActiveRecord::RecordInvalid.new(object_1))
      LS::Errors.expects(:report)
        event_1.process
      Timecop.return
    end

    should 'when a particular object and date combo report already exists' do
      object_1 = FactoryGirl.create(:object)
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        #{object_1.id},15,2015.01,1245.0,2015-10-28 12:00:00"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      CreateTotalByCategoryPerHourEvent.any_instance.expects(:not_duplicate_objects_dates?).returns(false)
      CreateTotalByCategoryPerHourEvent.expects(:create!).never
      CreateTotalByCategoryPerHourEvent.expects(:queue_action_event).never
      event.process
    end

    should 'when an empty array is passed' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>[]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end

    should 'when only headers are passed' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end

    should 'when nothing is passed' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>[""]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end

    should 'not process a bad object_id' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        666,1,2000.01,123.0,2015-10-28 12:00:00"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end

    should 'when object.id is nil' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        ,1,2000.01,123.0,2015-10-28 12:00:00"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end

    should 'when category_id is nil' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        123,,2000.01,123.0,2015-10-28 12:00:00"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end

    should 'when category_total_last_week is nil' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        123,456,,123.0,2015-10-28 12:00:00"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end

    should 'when hourly_total_last_week is nil' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        123,456,789,,2015-10-28 12:00:00"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end

    should 'when calculated_at is nil' do
      uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        123,456,780,123.0,"]}
      event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

      assert_no_difference("TotalByCategoryPerHour.count") do
        event.process
      end
    end
  end

  context 'when processing on different days' do
    should 'create a new report each day' do
      object_1 = FactoryGirl.create(:object)
      uploaded_file_1 = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        #{object_1.id},4444,2000.01,123.0,2015-10-03 12:00:00"]}
      uploaded_file_2 = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
        "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
        filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
        Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
        #{object_1.id},43,2000.01,123.0,2015-10-05 12:00:00"]}
      event_1 = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file_1 })
      event_2 = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file_2 })

      assert_difference("TotalByCategoryPerHour.count", 2) do
        Timecop.freeze(Time.parse("2015-10-04 12:00:00 EDT"))
        event_1.process
        Timecop.return

        Timecop.freeze(Time.parse("2015-10-08 12:00:00 EDT"))
        event_2.process
        Timecop.return
      end
    end

    context 'no queue_action_event' do
      should 'happen when a particular object and category combo report already exists' do
        object_1 = FactoryGirl.create(:object)
        object_2 = FactoryGirl.create(:object)
        object_3 = FactoryGirl.create(:object)
        uploaded_file = {"original_filename"=>"lulucat_total_by_category_per_hour_past_week_123456789lulu",
          "content_type"=>"application/octet-stream", "headers"=>"Content-Disposition: form-data; name=\"lulucat_data\";
          filename=\"/u/apps/lulucat/releases/20151021214648/tmp/api/lulucat_total_by_category_per_hour_past_week_123456789lulu\"\r\n
          Content-Type: application/octet-stream\r\n", "tempfile"=>["object_id,category_id,category_total_last_week,hourly_total_last_week,calculated_at\n
          #{object_1.id},4444,5000.01,1235.0,2015-10-7 12:00:00\n#{object_2.id},1,5600,423,2015-10-7 12:00:00\n#{object_3.id},1,3845.65,789.48,2015-10-7 12:00:00\n
          #{object_1.id},3,5600,423,2015-10-7 12:00:00\n#{object_3.id},1,3845.65,789.48,2015-10-13 12:00:00"]}
        # There are two rows with a duplicate object_id/category_id combo in the group of five rows
        event = CreateTotalByCategoryPerHourEvent.new({ payload: uploaded_file })

        assert_difference("TotalByCategoryPerHour.count", 4) do
          event.process
        end
        assert_equal 4, event.object_ids.count
        assert_not_equal 5, event.object_ids.count
      end
    end
  end
end