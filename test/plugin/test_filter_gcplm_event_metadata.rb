require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'
require "fluent/plugin/filter_gcplm"
require "hashdiff"

class FluentGCPLMVMTest < Test::Unit::TestCase
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

    sub_test_case "event metadata" do
    test 'all default metadata is added' do
      messages = [{
        "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
        "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
        "resource" => {
            "type" => "cloudsql_database",
            "labels" => {
                "project_id" => "development-198123",
                "region" => "us-central",
                "database_id" => "development-198123:testmysqllogstolm"
                }
            },
        "timestamp" => "2021-04-28T10:13:07.252739Z",
        "severity" => "INFO",
        "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
        "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
        }]

        expected = [{
            "_lm.resourceId" => {
                "system.gcp.resourceid" =>"development-198123:testmysqllogstolm",
                "system.gcp.projectId" =>"development-198123",
                "system.cloud.category" => 'GCP/CloudSQL'
                },
                "_type" => "cloudsql_database",
                "log_level" => "INFO",
                "_integration" => "gcp",
                "_resource.type" => "GCP",
                "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
            "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
            "timestamp" => "2021-04-28T10:13:07.252739Z",
            "resource.labels" => {
                "project_id" => "development-198123",
                "region" => "us-central",
                "database_id" => "development-198123:testmysqllogstolm"
                }
            }]
      actual = filter(messages)
      keys = []
      puts " actual : #{actual} \n expected : #{expected}"
        puts " hash diff : #{Hashdiff.diff(expected, actual)}"
      assert_equal([], Hashdiff.diff(expected,actual))

    end

    test 'skip severity by default if does not exist' do
        messages = [{
          "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
          "resource" => {
              "type" => "cloudsql_database",
              "labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"
                  }
              },
          "timestamp" => "2021-04-28T10:13:07.252739Z",
          "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
          "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
          }]

          expected = [{
              "_lm.resourceId" => {
                  "system.gcp.resourceid" =>"development-198123:testmysqllogstolm",
                  "system.gcp.projectId" =>"development-198123",
                  "system.cloud.category" => 'GCP/CloudSQL'
                  },
                  "_type" => "cloudsql_database",
                  "_integration" => "gcp",
                  "_resource.type" => "GCP",
                  "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
              "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
              "timestamp" => "2021-04-28T10:13:07.252739Z",
              "resource.labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"
                  }
              }]
        actual = filter(messages)
        keys = []
        puts " actual : #{actual} \n expected : #{expected}"
        puts " hash diff : #{Hashdiff.diff(expected, actual)}"
        assert_equal([], Hashdiff.diff(expected,actual))
      end

      test 'add default severity if enabled and severity does not exist' do
        messages = [{
          "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
          "resource" => {
              "type" => "cloudsql_database",
              "labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"
                  }
              },
          "timestamp" => "2021-04-28T10:13:07.252739Z",
          "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
          "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
          },
          {
            "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
            "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
            "resource" => {
                "type" => "cloudsql_database",
                "labels" => {
                    "project_id" => "development-198123",
                    "region" => "us-central",
                    "database_id" => "development-198123:testmysqllogstolm"
                    }
                },
            "severity" => "INFO",
            "timestamp" => "2021-04-28T10:13:07.252739Z",
            "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
            "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
            }]

          expected = [{
              "_lm.resourceId" => {
                  "system.gcp.resourceid" =>"development-198123:testmysqllogstolm",
                  "system.gcp.projectId" =>"development-198123",
                  "system.cloud.category" => 'GCP/CloudSQL'
                  },
                  "_type" => "cloudsql_database",
                  "log_level" => "DEFAULT",
                  "_integration" => "gcp",
                  "_resource.type" => "GCP",
                  "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
              "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
              "timestamp" => "2021-04-28T10:13:07.252739Z",
              "resource.labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"
                  }
              },
              {
                "_lm.resourceId" => {
                    "system.gcp.resourceid" =>"development-198123:testmysqllogstolm",
                    "system.gcp.projectId" =>"development-198123",
                    "system.cloud.category" => 'GCP/CloudSQL'
                    },
                    "_type" => "cloudsql_database",
                    "log_level" => "INFO",
                    "_integration" => "gcp",
                    "_resource.type" => "GCP",
                    "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
                "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
                "timestamp" => "2021-04-28T10:13:07.252739Z",
                "resource.labels" => {
                    "project_id" => "development-198123",
                    "region" => "us-central",
                    "database_id" => "development-198123:testmysqllogstolm"
                    }
                }]
        actual = filter(messages, %[
            use_default_severity true
          ])
        keys = []
        puts " actual : #{actual} \n expected : #{expected}"
        puts " hash diff : #{Hashdiff.diff(expected, actual)}"
        assert_equal([], Hashdiff.diff(expected,actual))
      end

      test 'custom metadata with Default severirty' do
        messages = [{
          "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
          "resource" => {
              "type" => "cloudsql_database",
              "labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"
                  }
              },
          "timestamp" => "2021-04-28T10:13:07.252739Z",
          "key1" => {
            "nest1" => {
                "nest2" => "nest2val"
            },
            "nest0" => "nest0val"
          },
          "key2" => ["a","b", "c"],
          "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
          "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
          }]

          expected = [{
              "_lm.resourceId" => {
                  "system.gcp.resourceid" =>"development-198123:testmysqllogstolm",
                  "system.gcp.projectId" =>"development-198123",
                  "system.cloud.category" => 'GCP/CloudSQL'
                  },
                  "log_level" => "DEFAULT",
                  "_integration" => "gcp",
                  "_resource.type" => "GCP",
              "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
              "timestamp" => "2021-04-28T10:13:07.252739Z",
                  "resource.labels.project_id" => "development-198123",
                  "resource.labels.region" => "us-central",
                  "key1.nest1.nest2" => "nest2val",
                  "key1.nest0" => "nest0val",
                  "key2" => ["a","b", "c"]

              }]
        actual = filter(messages,%[
            use_default_severity true
            metadata_keys resource.labels.region, resource.labels.project_id, severity, key1.nest1.nest2, key1.nest0, key2
          ])
        keys = []
        puts " actual : #{actual} \n expected : #{expected}"
        puts " hash diff : #{Hashdiff.diff(expected, actual)}"
        assert_equal([], Hashdiff.diff(expected,actual))
      end

      test "tenant_id specified in config" do
        messages = [{
          "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
          "resource" => {
              "type" => "cloudsql_database",
              "labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"
                  }
              },
          "timestamp" => "2021-04-28T10:13:07.252739Z",
          "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
          "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
          }]
        expected = [{
          "_lm.resourceId" => {
              "system.gcp.resourceid" =>"development-198123:testmysqllogstolm",
              "system.gcp.projectId" =>"development-198123",
              "system.cloud.category" => 'GCP/CloudSQL'
              },
              "_lm.tenantId" => "abc",
              "_integration" => "gcp",
              "_resource.type" => "GCP",
              "_type" => "cloudsql_database",
              "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
              "resource.labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"

              },
          "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "timestamp" => "2021-04-28T10:13:07.252739Z"
          }]
        actual = filter(messages,%[
          lm_tenant_id abc
          ])
        puts " actual : #{actual} \n expected : #{expected}"
        puts " hash diff : #{Hashdiff.diff(expected, actual)}"
        assert_equal([], Hashdiff.diff(expected[0],actual[0]))
      end

      test "tenant_id present in record" do
        messages = [{
          "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
          "resource" => {
              "type" => "cloudsql_database",
              "labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"
                  }
              },
          "timestamp" => "2021-04-28T10:13:07.252739Z",
          "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
          "_lm.tenantId" => "def",
          "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
          }]
        expected = [{
          "_lm.resourceId" => {
              "system.gcp.resourceid" =>"development-198123:testmysqllogstolm",
              "system.gcp.projectId" =>"development-198123",
              "system.cloud.category" => 'GCP/CloudSQL'
              },
              "_lm.tenantId" => "def",
              "_integration" => "gcp",
              "_resource.type" => "GCP",
              "_type" => "cloudsql_database",
              "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
              "resource.labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"

              },
          "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "timestamp" => "2021-04-28T10:13:07.252739Z"
          }]
        actual = filter(messages,%[
          lm_tenant_id abc
          ])
        puts " actual : #{actual} \n expected : #{expected}"
        puts " hash diff : #{Hashdiff.diff(expected, actual)}"
        assert_equal([], Hashdiff.diff(expected[0],actual[0]))
      end

      test " empty tenant_id specified" do
        messages = [{
          "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
          "resource" => {
              "type" => "cloudsql_database",
              "labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"
                  }
              },
          "timestamp" => "2021-04-28T10:13:07.252739Z",
          "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
          "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
          }]
        expected = [{
          "_lm.resourceId" => {
              "system.gcp.resourceid" =>"development-198123:testmysqllogstolm",
              "system.gcp.projectId" =>"development-198123",
              "system.cloud.category" => 'GCP/CloudSQL'
              },
              "_integration" => "gcp",
              "_resource.type" => "GCP",
              "_type" => "cloudsql_database",
              "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
              "resource.labels" => {
                  "project_id" => "development-198123",
                  "region" => "us-central",
                  "database_id" => "development-198123:testmysqllogstolm"

              },
          "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
          "timestamp" => "2021-04-28T10:13:07.252739Z"
          }]
        actual = filter(messages,%[
          lm_tenant_id   
          ])
        puts " actual : #{actual} \n expected : #{expected}"
        puts " hash diff : #{Hashdiff.diff(expected, actual)}"
        assert_equal([], Hashdiff.diff(expected[0],actual[0]))
      end




  end
end
