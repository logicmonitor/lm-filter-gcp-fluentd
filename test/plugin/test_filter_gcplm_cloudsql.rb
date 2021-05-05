require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'

require "fluent/plugin/filter_gcplm"

class FluentGCPLMCloudSqlTest < Test::Unit::TestCase
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
        test 'resourcemapping test for cloud_sql' do
                messages = [{
                    "textPayload" => "2021-04-27T14:34:16.352086Z 0 [ERROR] Could not successfully notify external app of semisync status change before timeout.",
                    "insertId" => "s=0f48625de67a4c96a1b506ffc3851ca5;i=1e47;b=1d5c153c19964ece83df6787dd571159;m=74f53eb;t=5c0f528216aa5;x=be395e5383cf012f-0-0@a1",
                    "resource" => {
                      "type" => "cloudsql_database",
                      "labels" => {
                        "region" => "asia",
                        "database_id" => "development-198123:rwaver-sql-1",
                        "project_id" => "development-198123"
                      }
                    },
                    "timestamp" => "2021-04-27T14:34:16.352421Z",
                    "severity" => "ERROR",
                    "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
                    "receiveTimestamp" => "2021-04-27T14:34:17.079988744Z"
                    }]

                expected = [{
                    "_lm.resourceId" => {
                        "system.gcp.resourceid" =>"development-198123:rwaver-sql-1",
                        "system.cloud.category" => 'GCP/CloudSQL'
                        },
                    "message" =>"2021-04-27T14:34:16.352086Z 0 [ERROR] Could not successfully notify external app of semisync status change before timeout.",
                    "insertId" => "s=0f48625de67a4c96a1b506ffc3851ca5;i=1e47;b=1d5c153c19964ece83df6787dd571159;m=74f53eb;t=5c0f528216aa5;x=be395e5383cf012f-0-0@a1",
                    "resource" => {
                      "type" => "cloudsql_database",
                      "labels" => {
                        "region" => "asia",
                        "database_id" => "development-198123:rwaver-sql-1",
                        "project_id" => "development-198123"
                      }
                    },
                    "timestamp" => "2021-04-27T14:34:16.352421Z",
                    "severity" => "ERROR",
                    "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
                    "receiveTimestamp" => "2021-04-27T14:34:17.079988744Z",
                    "textPayload" => "2021-04-27T14:34:16.352086Z 0 [ERROR] Could not successfully notify external app of semisync status change before timeout.",
                    }]
                actual = filter(messages)
                actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
        end
    end
end