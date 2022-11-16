require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'

require "fluent/plugin/filter_gcplm"

class FluentGCPLMCloudRunTest < Test::Unit::TestCase
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
        test 'resourcemapping test for cloud_run' do
                messages = [{
                    "insertId"=> "6098cb340009e08b05e5ef54",
                    "httpRequest"=> {
                      "requestMethod"=> "GET",
                      "requestUrl"=> "https://hello-tkbjruipeq-uc.a.run.app/",
                      "requestSize"=> "345",
                      "status"=> 500,
                      "userAgent"=> "LogicMonitor SiteMonitor/1.0",
                      "remoteIp"=> "34.223.95.106",
                      "serverIp"=> "216.239.36.53",
                      "latency"=> "0.004592759s",
                      "protocol"=> "HTTP/1.1"
                      },
                    "resource"=> {
                      "type"=> "cloud_run_revision",
                      "labels"=> {
                        "project_id"=> "development-198123",
                        "configuration_name"=> "hello",
                        "location"=> "us-central1",
                        "service_name"=> "hello",
                        "revision_name"=> "hello-00002-huy"
                        }
                      },
                    "timestamp"=> "2021-05-10T05:57:08.647307Z",
                    "severity"=> "INFO",
                    "labels"=> {
                      "instanceId"=> "00bf4bf"
                      },
                    "logName"=> "projects/development-198123/logs/run.googleapis.com%2Frequests",
                    "trace"=> "projects/development-198123/traces/abf1c36d7e868424827714c3e4438d8a",
                    "receiveTimestamp"=> "2021-05-10T05:57:08.648855093Z"
                    }]

                expected = [{
                    "_lm.resourceId" => {
                        "system.gcp.resourcename" => "hello",
                        "system.cloud.category" => 'GCP/CloudRun',
                        "system.gcp.projectId" =>"development-198123"
                        },
                    "message" => "{\"requestMethod\":\"GET\",\"requestUrl\":\"https:\/\/hello-tkbjruipeq-uc.a.run.app\/\",\"requestSize\":\"345\",\"status\":500,\"userAgent\":\"LogicMonitor SiteMonitor\/1.0\",\"remoteIp\":\"34.223.95.106\",\"serverIp\":\"216.239.36.53\",\"latency\":\"0.004592759s\",\"protocol\":\"HTTP\/1.1\"}",
                    "timestamp" => "2021-05-10T05:57:08.647307Z"
                    }]
                actual = filter(messages)
                actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
                actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
        end

        test 'resource and message mapping test for cloud_run' do
                messages = [{
                    "textPayload"=> "> logging-manual@ start /usr/src/app",
                    "insertId"=> "6098be59000e0e9e29e705bc",
                    "resource"=> {
                      "type"=> "cloud_run_revision",
                      "labels"=> {
                        "location"=> "us-central1",
                        "project_id"=> "development-198123",
                        "configuration_name"=> "logging-manual",
                        "revision_name"=> "logging-manual-00002-non",
                        "service_name"=> "logging-manual"
                        }
                      },
                    "timestamp"=> "2021-05-10T05:02:17.921246Z",
                    "labels"=> {
                      "instanceId"=> "00bf4bf"
                      },
                    "logName"=> "projects/development-198123/logs/run.googleapis.com%2Fstdout",
                    "receiveTimestamp"=> "2021-05-10T05:02:17.923289983Z"
                    }]

                expected = [{
                    "_lm.resourceId" => {
                        "system.gcp.resourcename" => "logging-manual",
                        "system.cloud.category" => 'GCP/CloudRun',
                        "system.gcp.projectId" =>"development-198123"
                    },
                    "message" => "> logging-manual@ start /usr/src/app",
                    "timestamp" => "2021-05-10T05:02:17.921246Z"
                    }]
                actual = filter(messages)
                actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
                actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
        end
    end
end