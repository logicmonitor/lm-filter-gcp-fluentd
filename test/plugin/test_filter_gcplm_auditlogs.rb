require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'

require "fluent/plugin/filter_gcplm"

class FluentGCPLMAuditLogsTest < Test::Unit::TestCase
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
        test 'resourcemapping test for audit logs' do
                messages = [{
                   "protoPayload" => {
                     "@type" => "type.googleapis.com/google.cloud.audit.AuditLog",
                     "status" => {
                       "message" => "Instance Group Manager 'projects/272452262635/zones/us-central1-c/instanceGroupManagers/aef-backend-20180822t164014-00' initiated recreateInstance on instance 'projects/272452262635/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j'. Reason: instance's current action is VERIFYING, but health status is TIMEOUT."
                       },
                     "authenticationInfo" => {
                       "principalEmail" => "system@google.com"
                       },
                     "serviceName" => "compute.googleapis.com",
                     "methodName" => "compute.instances.repair.recreateInstance",
                     "resourceName" => "projects/development-198123/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j",
                     "request" => {
                       "@type" => "type.googleapis.com/compute.instances.repair.recreateInstance"
                       }
                     },
                   "insertId" => "bdl1yydsszy",
                   "resource" => {
                     "type" => "gce_instance",
                     "labels" => {
                      "zone" => "us-central1-c",
                      "project_id" => "development-198123",
                      "instance_id" => "5229353360185505344"
                      }
                     },
                   "timestamp" => "2021-04-27T06:44:51.535974Z",
                   "severity" => "INFO",
                   "logName" => "projects/development-198123/logs/cloudaudit.googleapis.com%2Fsystem_event",
                   "operation" => {
                    "id" => "repair-1619505891524-5c0ee99603e9a-862341d5-24923c3c",
                    "producer" => "compute.instances.repair.recreateInstance",
                    "first" => true,
                    "last" => true
                     },
                   "receiveTimestamp" => "2021-04-27T06:44:51.904218698Z"
                   },
                {
                   "protoPayload" => {
                     "status" => {
                       "message" => "Instance Group Manager 'projects/272452262635/zones/us-central1-c/instanceGroupManagers/aef-backend-20180822t164014-00' initiated recreateInstance on instance 'projects/272452262635/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j'. Reason: instance's current action is VERIFYING, but health status is TIMEOUT."
                       },
                     "authenticationInfo" => {
                       "principalEmail" => "system@google.com"
                       },
                     "serviceName" => "compute.googleapis.com",
                     "methodName" => "compute.instances.repair.recreateInstance",
                     "resourceName" => "projects/development-198123/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j",
                     "request" => {
                       "@type" => "type.googleapis.com/compute.instances.repair.recreateInstance"
                       }
                     },
                   "insertId" => "bdl1yydsszy",
                   "resource" => {
                     "type" => "gce_instance",
                     "labels" => {
                      "zone" => "us-central1-c",
                      "project_id" => "development-198123"
                      }
                     },
                   "timestamp" => "2021-04-27T06:44:51.535974Z",
                   "severity" => "INFO",
                   "logName" => "projects/development-198123/logs/cloudaudit.googleapis.com%2Fsystem_event",
                   "operation" => {
                    "id" => "repair-1619505891524-5c0ee99603e9a-862341d5-24923c3c",
                    "producer" => "compute.instances.repair.recreateInstance",
                    "first" => true,
                    "last" => true
                     },
                   "receiveTimestamp" => "2021-04-27T06:44:51.904218698Z"
                    }]

                expected = [{
                    "_lm.resourceId" => {
                        "system.gcp.projectId" =>"development-198123",
                        "system.cloud.category" => 'GCP/LMAccount'
                        },
                    "message" =>"{\"@type\": \"type.googleapis.com/google.cloud.audit.AuditLog\",\"status\": {\"message\": \"Instance Group Manager 'projects/272452262635/zones/us-central1-c/instanceGroupManagers/aef-backend-20180822t164014-00' initiated recreateInstance on instance 'projects/272452262635/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j'. Reason: instance's current action is VERIFYING, but health status is TIMEOUT.\"},\"authenticationInfo\": {\"principalEmail\": \"system@google.com\"},\"serviceName\": \"compute.googleapis.com\",\"methodName\": \"compute.instances.repair.recreateInstance\",\"resourceName\": \"projects/development-198123/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j\",\"request\": {\"@type\": \"type.googleapis.com/compute.instances.repair.recreateInstance\"}}",
                    "timestamp" => "2021-04-27T06:44:51.535974Z"
                    },
                    {
                    "_lm.resourceId" => {},
                    "message" =>"{\"@type\": \"type.googleapis.com/google.cloud.audit.AuditLog\",\"status\": {\"message\": \"Instance Group Manager 'projects/272452262635/zones/us-central1-c/instanceGroupManagers/aef-backend-20180822t164014-00' initiated recreateInstance on instance 'projects/272452262635/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j'. Reason: instance's current action is VERIFYING, but health status is TIMEOUT.\"},\"authenticationInfo\": {\"principalEmail\": \"system@google.com\"},\"serviceName\": \"compute.googleapis.com\",\"methodName\": \"compute.instances.repair.recreateInstance\",\"resourceName\": \"projects/development-198123/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j\",\"request\": {\"@type\": \"type.googleapis.com/compute.instances.repair.recreateInstance\"}}",
                    "timestamp" => "2021-04-27T06:44:51.535974Z"
                    }]
                actual = filter(messages)
                actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
        end
    end
end

