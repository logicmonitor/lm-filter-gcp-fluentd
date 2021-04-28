require 'test/unit'
require "fluent/test"
require 'fluent/test/driver/filter'

require "../../lib/fluent/plugin/filter_gcplm"

class FluentGCPLMTest < Test::Unit::TestCase
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
    test 'resourcemapping test for compute engine' do
      messages = [{
       "textPayload" => "some text",
       "resource" => {
            "type" => "gce_instance",
            "labels" => {
                "instance_id" => "gce_instance_id"
                }
            }
       },
       {
       "textPayload" => "some text",
       "resource" => {
           "type" => "gce_instance",
           },
       "labels" =>{
           "compute.googleapis.com/resource_name" =>"gce_resource_name"
           }
       }]
      expected = [{
        "_lm.resourceId" => {
            "system.gcp.resourceid" =>"gce_instance_id"
            },
        "message" =>"some text",
        "resource" => {
            "labels" => {
                "instance_id" =>"gce_instance_id"
                },
            "type" =>"gce_instance"
            },
        "textPayload" =>"some text"
        },
        {
        "_lm.resourceId" => {
            "system.gcp.resourcename" =>"gce_resource_name"
            },
        "message" =>"some text",
        "resource" => {
            "type" =>"gce_instance"
            },
        "textPayload" =>"some text"
        }]
      actual = filter(messages)
      actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
    end

    test 'resourcemapping test for cloud functions' do
        messages = [{
            "textPayload" => "some text message",
            "resource" => {
                "type" => "cloud_function",
                "labels" => {
                    "region" => "some_region",
                    "function_name" => "some_function_name",
                    "project_id" => "some_project_id"
                    }
                }
            }]

        expected = [{
            "_lm.resourceId" => {
                "system.gcp.resourcename" =>"projects/some_project_id/locations/some_region/functions/some_function_name"
                },
            "message" =>"some text message",
            "resource" => {
                "type" =>"cloud_function"
                },
            "textPayload" =>"some text message"
        }]
        actual = filter(messages)
        actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
    end

    test 'resourcemapping test for cloud_sql' do
        messages = [{
            "textPayload" => "some text message",
            "resource" => {
                "type" => "cloudsql_database",
                "labels" => {
                    "database_id" => "project_id:db_name",
                    }
                }
            }]

        expected = [{
            "_lm.resourceId" => {
                "system.gcp.resourceid" =>"project_id:db_name"
                },
            "message" =>"some text message",
            "resource" => {
                "type" =>"cloudsql_database"
                },
            "textPayload" =>"some text message"
            }]
        actual = filter(messages)
        actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
    end

    test 'resourcemapping test for audit logs' do
        messages = [{
            "protoPayload" => {
                "@type" => "type.googleapis.com/google.cloud.audit.AuditLog",
            "status" => {
                "message" => "some message"
                }
            },
            "resource" => {
                "type" => "gce_instance",
                "labels" => {
                    "project_id" => "project_id",
                    "instance_id" => "5229353360185505344"
                    }
                }
            },
        {
           "protoPayload" => {
                "status" => {
                    "message" => "some message"
                    }
                },
           "resource" => {
            "type" => "gce_instance",
                "labels" => {
                    "project_id" => "project_id"
                    }
                }
            }]

        expected = [{
            "_lm.resourceId" => {
                "system.gcp.projectId" =>"project_id",
                "system.cloud.category" => 'GCP/LMAccount'
                },
            "message" =>"{\"@type\":\"type.googleapis.com/google.cloud.audit.AuditLog\",\"status\":{\"message\":\"some message\"}}",
            "resource" => {
                "type" => "gce_instance",
                "labels" => {
                    "project_id" => "project_id",
                    "instance_id" => "5229353360185505344"
                    }
                }
            },
            {
            "_lm.resourceId" => {},
            "message" =>"{\"status\":{\"message\":\"some message\"}}",
            "resource" => {
                "type" => "gce_instance",
                "labels" => {
                    "project_id" => "project_id"
                    }
                }
            }]
        actual = filter(messages)
        actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
        end
    end

  sub_test_case "message_mapping" do
    test 'text payload test' do
        messages = [{
            "textPayload" => "some text message",
            "resource" => {
                "type" => "cloudsql_database",
                "labels" => {
                    "database_id" => "project_id:db_name",
                    }
                }
            }]

            expected = [{
                "_lm.resourceId" => {
                    "system.gcp.resourceid" =>"project_id:db_name"
                    },
                "message" =>"some text message",
                "resource" => {
                    "type" =>"cloudsql_database"
                    },
                "textPayload" =>"some text message"
            }]
        actual = filter(messages)
        actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
    end

    test 'json payload test' do
        messages = [{
            "jsonPayload" => {"some_key1" => "some_value1","some_key2" => "some_value2"},
            "resource" => {
                "type" => "cloudsql_database",
                "labels" => {
                    "database_id" => "project_id:db_name",
                    }
                }
            }]

        expected=[{
            "_lm.resourceId" => {
                "system.gcp.resourceid" =>"project_id:db_name"
                },
            "message" =>"{\"some_key1\":\"some_value1\",\"some_key2\":\"some_value2\"}",
            "resource" => {
                "type" =>"cloudsql_database"
                },
            "jsonPayload" => {"some_key1" => "some_value1","some_key2" => "some_value2"}
            }]
        actual = filter(messages)
        actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
    end

    test 'proto payload test from audit logs' do
        messages = [{
            "protoPayload" => {"@type" => "type.googleapis.com/google.cloud.audit.AuditLog","some_key2" => "some_value2"},
            "resource" => {
                "type" => "cloudsql_database",
                "labels" => {
                    "database_id" => "project_id:db_name",
                    }
                }
            }]

        expected = [{
        "_lm.resourceId" => {
            "system.gcp.resourceid" =>"project_id:db_name"
            },
        "message" =>"{\"@type\":\"type.googleapis.com/google.cloud.audit.AuditLog\",\"some_key2\":\"some_value2\"}",
        "resource" => {
            "type" =>"cloudsql_database"
            },
        "jsonPayload" => {"@type" => "type.googleapis.com/google.cloud.audit.AuditLog","some_key2" => "some_value2"}
        }]
        actual = filter(messages)
        actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
    end
  end

  sub_test_case "anything other than accepted scenario" do
    test 'text null message and empty _lm.resourceId test' do
      messages = [{
        "some_payload" => "",
        "resource" => {
            "type" => "some_other_service",
            "labels" => {
                "service_id" => "some_resource_id",
                }
            }
        }]
      expected = [{
        "_lm.resourceId" =>{},
        "message" =>nil,
        "resource" => {
            "type" =>"some_other_service"
            },
        "some_payload" => ""
        }]
      actual = filter(messages)
      actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
      actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
    end
  end
end