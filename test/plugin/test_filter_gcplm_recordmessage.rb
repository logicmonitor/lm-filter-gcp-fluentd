require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'

require "fluent/plugin/filter_gcplm"

class FluentGCPLMMessageTest < Test::Unit::TestCase
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

  sub_test_case "message_mapping" do
      test 'text payload test' do
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
                  "system.gcp.resourceid" =>"development-198123:testmysqllogstol"
                  },
              "message" =>"Permission monitoring.timeSeries.create denied (or the resource may not exist)",
              "insertId" => "s=eec12ab3a95840a99afd10c76b210343;i=1deb;b=5db8d88851ef4e938abe5374adb7ccab;m=71f6a52;t=5c105a0054e03;x=dfabe513f9cebad8-0-0@a1",
              "resource" => {
                  "type" => "cloudsql_database",
                  "labels" => {
                      "project_id" => "development-198123",
                      "region" => "us-central",
                      "database_id" => "development-198123:testmysqllogstolm"
                      }
                  },
              "textPayload" => "Permission monitoring.timeSeries.create denied (or the resource may not exist)",
              "timestamp" => "2021-04-28T10:13:07.252739Z",
              "severity" => "INFO",
              "logName" => "projects/development-198123/logs/cloudsql.googleapis.com%2Fmysql.err",
              "receiveTimestamp" => "2021-04-28T10:13:08.349276124Z"
              }]
          actual = filter(messages)
          actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
      end

      test 'json payload test' do
          messages = [{
              "insertId" => "1n469d5g101b4pi",
              "jsonPayload" => {
                  "message" => "startup-script: chmod: changing permissions of '/etc/systemd/system/dbus-org.freedesktop.network1.service': Read-only file system",
                  "localTimestamp" => "2021-04-28T14:32:03.2422Z"
                  },
              "resource" => {
                  "type" => "gce_instance",
                  "labels" => {
                      "project_id" => "development-198123",
                      "zone" => "us-central1-c",
                      "instance_id" => "7637443554836808513"
                  }
              },
              "timestamp" => "2021-04-28T14:32:03.243408039Z",
              "severity" => "INFO",
              "logName" => "projects/development-198123/logs/GCEMetadataScripts",
              "sourceLocation" => {
                  "file" => "main.go",
                  "line" => "475",
                  "function" => "main.main"
                  },
              "receiveTimestamp" => "2021-04-28T14:32:04.142488955Z"
              }]

          expected=[{
              "_lm.resourceId" => {
                  "system.gcp.resourceid" =>"7637443554836808513"
                  },
              "message" =>"{\"message\":\"startup-script: chmod: changing permissions of '/etc/systemd/system/dbus-org.freedesktop.network1.service': Read-only file system\",\"localTimestamp\":\"2021-04-28T14:32:03.2422Z\"}",
              "jsonPayload" => {
                  "message" => "startup-script: chmod: changing permissions of '/etc/systemd/system/dbus-org.freedesktop.network1.service': Read-only file system",
                  "localTimestamp" => "2021-04-28T14:32:03.2422Z"
                  },
              "resource" => {
                  "type" => "gce_instance",
                  "labels" => {
                      "project_id" => "development-198123",
                      "zone" => "us-central1-c",
                      "instance_id" => "7637443554836808513"
                  }
              },
              "timestamp" => "2021-04-28T14:32:03.243408039Z",
              "severity" => "INFO",
              "logName" => "projects/development-198123/logs/GCEMetadataScripts",
              "sourceLocation" => {
                  "file" => "main.go",
                  "line" => "475",
                  "function" => "main.main"
                  },
              "receiveTimestamp" => "2021-04-28T14:32:04.142488955Z"
              }]
          actual = filter(messages)
          actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
      end

      test 'proto payload test from audit logs' do
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
              }]

          expected = [{
              "_lm.resourceId" => {
                  "system.gcp.projectId" =>"development-198123",
                  "system.cloud.category" => 'GCP/LMAccount'
                  },
              "message" =>"{\"@type\":\"type.googleapis.com/google.cloud.audit.AuditLog\",\"status\":{\"message\":\"Instance Group Manager 'projects/272452262635/zones/us-central1-c/instanceGroupManagers/aef-backend-20180822t164014-00' initiated recreateInstance on instance 'projects/272452262635/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j'. Reason: instance's current action is VERIFYING, but health status is TIMEOUT.\"},\"authenticationInfo\":{\"principalEmail\":\"system@google.com\"},\"serviceName\":\"compute.googleapis.com\",\"methodName\":\"compute.instances.repair.recreateInstance\",\"resourceName\":\"projects/development-198123/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j\",\"request\":{\"@type\":\"type.googleapis.com/compute.instances.repair.recreateInstance\"}}",
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
          }]
          actual = filter(messages)
          actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
      end
    end

    sub_test_case "receiving log from resource other than expected" do
      test 'message and empty _lm.resourceId test' do
        messages = [{
          "textPayload" => "ERROR: logging before flag.Parse: E0428 14:44:29.617217       1 reflector.go:205] github.com/logicmonitor/k8s-argus/pkg/argus.go:200: Failed to list *v1beta2.Deployment: the server could not find the requested resource\n",
          "insertId" => "16gkumeg36ecj0z",
          "resource" => {
              "type" => "container",
              "labels" => {
                  "cluster_name" => "k8s-demo",
                  "instance_id" => "1495050283252975838",
                  "container_name" => "argus",
                  "zone" => "us-central1-a",
                  "project_id" => "development-198123",
                  "pod_id" => "argus-78cc587ccf-b9j44",
                  "namespace_id" => "default"
              }
          },
          "timestamp" => "2021-04-28T14:44:29.617390650Z",
          "severity" => "ERROR",
          "labels" => {
              "compute.googleapis.com/resource_name" => "gke-k8s-demo-default-pool-8eece21e-re2i",
              "container.googleapis.com/pod_name" => "argus-78cc587ccf-b9j44",
              "container.googleapis.com/namespace_name" => "default",
              "container.googleapis.com/stream" => "stderr"
              },
          "logName" => "projects/development-198123/logs/argus",
          "receiveTimestamp" => "2021-04-28T14:44:34.935751956Z"
          }]
        expected = [{
          "_lm.resourceId" => {},
          "message" => "ERROR: logging before flag.Parse: E0428 14:44:29.617217       1 reflector.go:205] github.com/logicmonitor/k8s-argus/pkg/argus.go:200: Failed to list *v1beta2.Deployment: the server could not find the requested resource\n",
          "textPayload" => "ERROR: logging before flag.Parse: E0428 14:44:29.617217       1 reflector.go:205] github.com/logicmonitor/k8s-argus/pkg/argus.go:200: Failed to list *v1beta2.Deployment: the server could not find the requested resource\n",
          "insertId" => "16gkumeg36ecj0z",
          "resource" => {
              "type" => "container",
              "labels" => {
                  "cluster_name" => "k8s-demo",
                  "instance_id" => "1495050283252975838",
                  "container_name" => "argus",
                  "zone" => "us-central1-a",
                  "project_id" => "development-198123",
                  "pod_id" => "argus-78cc587ccf-b9j44",
                  "namespace_id" => "default"
              }
          },
          "timestamp" => "2021-04-28T14:44:29.617390650Z",
          "severity" => "ERROR",
          "labels" => {
              "compute.googleapis.com/resource_name" => "gke-k8s-demo-default-pool-8eece21e-re2i",
              "container.googleapis.com/pod_name" => "argus-78cc587ccf-b9j44",
              "container.googleapis.com/namespace_name" => "default",
              "container.googleapis.com/stream" => "stderr"
              },
          "logName" => "projects/development-198123/logs/argus",
          "receiveTimestamp" => "2021-04-28T14:44:34.935751956Z"
          }]
        actual = filter(messages)
        actual.each_with_index { |val, index| assert_equal(expected[index]["message"], actual[index]["message"]) }
        actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
      end
    end
end