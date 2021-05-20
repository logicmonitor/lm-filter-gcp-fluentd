require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'

require "fluent/plugin/filter_gcplm"

class FluentGCPLMCloudComposerTest < Test::Unit::TestCase
  include Fluent::Test::Helpers
  def setup
    Fluent::Test.setup # this is required to setup router and others
  end

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::GCPLMFilter).configure(conf)
  end

  def filter(messages, conf = '')
    d = create_driver(conf)
    d.run(default_tag: 'input.access') do
      messages.each do |message|
        d.feed(message)
      end
    end
    d.filtered_records
  end

    sub_test_case "resource_mapping" do
        test 'resourcemapping test for cloud_composer' do
                messages = [{
                    "textPayload"=> "[2021-05-11 11:16:19,726] {dag_processing.py:1316} INFO - Failing jobs without heartbeat after 2021-05-11 11:11:19.726821+00:00\n",
                    "insertId"=> "bxyt8bg68znrkk",
                    "resource"=> {
                      "type"=> "cloud_composer_environment",
                      "labels"=> {
                        "location"=> "asia-south1",
                        "project_id"=> "development-198123",
                        "environment_name"=> "example-environment"
                        }
                      },
                    "timestamp"=> "2021-05-11T11:17:12.683674946Z",
                    "severity"=> "INFO",
                    "labels"=> {
                      "process"=> "dag-processor-manager"
                      },
                    "logName"=> "projects/development-198123/logs/dag-processor-manager",
                    "receiveTimestamp"=> "2021-05-11T11:17:16.328358495Z"
                    }]

                expected = [{
                    "_lm.resourceId" => {
                        "system.gcp.resourcename" => "projects/development-198123/locations/asia-south1/environments/example-environment",
                        "system.cloud.category" => "GCP/CloudComposer"
                        },
                    "message" => "[2021-05-11 11:16:19,726] {dag_processing.py:1316} INFO - Failing jobs without heartbeat after 2021-05-11 11:11:19.726821+00:00\n",
                    "timestamp" => "2021-05-11T11:17:12.683674946Z"
                    }]
                actual = filter(messages)
                actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
                actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
        end
    end
end