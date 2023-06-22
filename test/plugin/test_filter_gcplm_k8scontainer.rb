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
        test 'resourcemapping test for k8s containers' do
                messages = [{
                              "insertId": "709zzef6lbx08uzh",
                              "textPayload": "test message for k8s container",
                              "resource": {
                                "type": "k8s_container",
                                "labels": {
                                  "location": "us-central1-c",
                                  "project_id": "development-198123",
                                  "pod_name": "frontend-67dcfff596-rvp7k",
                                  "namespace_name": "default",
                                  "container_name": "server",
                                  "cluster_name": "cko-demo"
                                }
                              },
                              "timestamp": "2023-06-15T05:13:22.363399929Z",
                              "severity": "DEBUG",
                              "labels": {
                                "compute.googleapis.com/resource_name": "gke-cko-demo-pool-3-921af957-acz0",
                                "k8s-pod/pod-template-hash": "67dcfff596",
                                "k8s-pod/app": "frontend",
                                "k8s-pod/skaffold_dev/run-id": "451957a7-926a-475e-9ed2-fff07cfd7bfe"
                              },
                              "logName": "projects/development-198123/logs/stdout",
                              "receiveTimestamp": "2023-06-15T05:13:25.508807853Z"
                            }]

                expected = [{
                    "_lm.resourceId" => {
                        "system.gcp.resourcename" =>"gke-cko-demo-pool-3-921af957-acz0",
                        "system.cloud.category" => 'GCP/ComputeEngine'
                        },
                    "message" =>"test message for k8s container",
                    "timestamp" => "2023-06-15T05:13:22.363399929Z"
                }]
                actual = filter(messages)
                actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
        end
    end
end