require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'

require "fluent/plugin/filter_gcplm"

class FluentGCPLMCFTest < Test::Unit::TestCase
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
        test 'resourcemapping test for cloud functions' do
                messages = [{
                    "textPayload" => "Function execution took 200 ms, finished with status: 'ok'",
                    "insertId" => "000000-d8f6e03b-271c-4c7d-9780-4c63d37ccda7",
                    "resource" => {
                      "type" => "cloud_function",
                      "labels" => {
                        "function_name" => "testfunctionlogsToPubsub",
                        "region" => "asia-south1",
                        "project_id" => "logicmonitor.com:api-project-650342240768"
                        }
                      },
                    "timestamp" => "2021-04-15T06:14:10.997438123Z",
                    "severity" => "DEBUG",
                    "labels" => {
                      "execution_id" => "d4mdjbva6jz2"
                      },
                    "logName" => "projects/logicmonitor.com:api-project-650342240768/logs/cloudfunctions.googleapis.com%2Fcloud-functions",
                    "trace" => "projects/logicmonitor.com:api-project-650342240768/traces/72e2cdf9e92a630b981f61e9e498f2e0",
                    "receiveTimestamp" => "2021-04-08T09:59:25.195011779Z"
                    }]

                expected = [{
                    "_lm.resourceId" => {
                        "system.gcp.resourcename" =>"projects/logicmonitor.com:api-project-650342240768/locations/asia-south1/functions/testfunctionlogsToPubsub",
                        "system.cloud.category" => 'GCP/CloudFunction'
                        },
                    "message" =>"Function execution took 200 ms, finished with status: 'ok'",
                    "insertId" => "000000-d8f6e03b-271c-4c7d-9780-4c63d37ccda7",
                    "resource" => {
                      "type" => "cloud_function",
                      "labels" => {
                        "function_name" => "testfunctionlogsToPubsub",
                        "region" => "asia-south1",
                        "project_id" => "logicmonitor.com:api-project-650342240768"
                        }
                      },
                    "timestamp" => "2021-04-15T06:14:10.997438123Z",
                    "severity" => "DEBUG",
                    "labels" => {
                      "execution_id" => "d4mdjbva6jz2"
                      },
                    "logName" => "projects/logicmonitor.com:api-project-650342240768/logs/cloudfunctions.googleapis.com%2Fcloud-functions",
                    "trace" => "projects/logicmonitor.com:api-project-650342240768/traces/72e2cdf9e92a630b981f61e9e498f2e0",
                    "receiveTimestamp" => "2021-04-08T09:59:25.195011779Z",
                    "textPayload" => "Function execution took 200 ms, finished with status: 'ok'",
                }]
                actual = filter(messages)
                actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
        end
    end
end