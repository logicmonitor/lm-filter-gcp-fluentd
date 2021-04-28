require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'

require "fluent/plugin/filter_gcplm"

class FluentGCPLMTest < Test::Unit::TestCase
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
    test 'resourcemapping test for compute engine' do
      messages = [{
        "insertId" => "1gq1afxg1xdtkqm",
        "jsonPayload" => {
            "PRIORITY" => "6",
            "_HOSTNAME" => "gke-k8s-demo-default-pool-8eece21e-z6er",
            "_GID" => "0",
            "_BOOT_ID" => "fa612042f606422a80b4c64c2c642eb5",
            "_SYSTEMD_SLICE" => "system.slice",
            "SYSLOG_IDENTIFIER" => "kubelet",
            "_CAP_EFFECTIVE" => "3fffffffff",
            "_STREAM_ID" => "6aba8f44b68e4718a13999c63d142e28",
            "_CMDLINE" => "/home/kubernetes/bin/kubelet --v=2 --cloud-provider=gce --experimental-check-node-capabilities-before-mount=true --experimental-mounter-path=/home/kubernetes/containerized_mounter/mounter --cert-dir=/var/lib/kubelet/pki/ --cni-bin-dir=/home/kubernetes/bin --kubeconfig=/var/lib/kubelet/kubeconfig --image-pull-progress-deadline=5m --experimental-kernel-memcg-notification=true --max-pods=110 --non-masquerade-cidr=0.0.0.0/0 --network-plugin=kubenet --node-labels=beta.kubernetes.io/fluentd-ds-ready=true,cloud.google.com/gke-nodepool=default-pool,cloud.google.com/gke-os-distribution=cos --volume-plugin-dir=/home/kubernetes/flexvolume --bootstrap-kubeconfig=/var/lib/kubelet/bootstrap-kubeconfig --node-status-max-images=25 --registry-qps=10 --registry-burst=20 --config /home/kubernetes/kubelet-config.yaml --pod-sysctls=net.core.somaxconn=1024,net.ipv4.conf.all.accept_redirects=0,net.ipv4.conf.all.forwarding=1,net.ipv4.conf.all.route_localnet=1,net.ipv4.conf.default.forwarding=1,net.ipv4.ip_forward=1,net.ipv4.tcp_fin_timeout=60,net.ipv4.tcp_keepalive_intvl=75,net.ipv4.tcp_keepalive_probes=9,net.ipv4.tcp_keepalive_time=7200,net.ipv4.tcp_rmem=4096 87380 6291456,net.ipv4.tcp_syn_retries=6,net.ipv4.tcp_tw_reuse=0,net.ipv4.tcp_wmem=4096 16384 4194304,net.ipv4.udp_rmem_min=4096,net.ipv4.udp_wmem_min=4096,net.ipv6.conf.default.accept_ra=0,net.netfilter.nf_conntrack_generic_timeout=600,net.netfilter.nf_conntrack_tcp_timeout_close_wait=3600,net.netfilter.nf_conntrack_tcp_timeout_established=86400",
            "_PID" => "1243",
            "MESSAGE" => "I0415 06:18:20.943836    1243 kubelet_getters.go:177] status for pod kube-proxy-gke-k8s-demo-default-pool-8eece21e-z6er updated to {Running [{Initialized True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:43 +0000 UTC  } {Ready True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:44 +0000 UTC  } {ContainersReady True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:44 +0000 UTC  } {PodScheduled True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:43 +0000 UTC  }]    10.128.15.198 10.128.15.198 2021-01-21 17:17:43 +0000 UTC [] [{kube-proxy {nil &ContainerStateRunning{StartedAt:2021-01-21 17:17:44 +0000 UTC,} nil} {nil nil nil} true 0 gke.gcr.io/kube-proxy:v1.15.12-gke.6002 docker://sha256:d97a2b9779ce0e53fc2ae82dc68a6ca97c1e8cbe111f99814715b730c28523e7 docker://0a8d009d27318e01ee97899e70b2a9d9de27d62a7baf781574f648a2cb5e9aa1}] Burstable}"
            },
        "resource" => {
            "type" => "gce_instance",
            "labels" => {
                "project_id" => "development-198123",
                "zone" => "us-central1-a",
                "instance_id" => "1437928523886234104"
                }
            },
        "timestamp" => "2021-04-15T06:18:20.943939Z",
        "logName" => "projects/development-198123/logs/kubelet",
        "receiveTimestamp" => "2021-04-15T06:18:26.830083776Z"
        },
        {
        "textPayload" => "BaudRate/Actual (115200/115200) = 100%\r\n",
        "insertId" => "181",
        "resource" => {
            "type" => "gce_instance"
            },
        "timestamp" => "2021-04-28T13:08:27.808628469Z",
        "labels" => {
            "compute.googleapis.com/resource_name" => "gke-standard-cluster-1-a-default-pool-96c859da-e9e7"
        },
        "logName" => "projects/development-198123/logs/serialconsole.googleapis.com%2Fserial_port_1_output",
        "receiveTimestamp" => "2021-04-28T13:08:29.563802575Z"
        }]
        
      expected = [{
        "_lm.resourceId" => {
            "system.gcp.resourceid" =>"1437928523886234104"
            },
        "message" =>"\"PRIORITY\": \"6\",\"_HOSTNAME\": \"gke-k8s-demo-default-pool-8eece21e-z6er\",\"_GID\": \"0\",\"_BOOT_ID\": \"fa612042f606422a80b4c64c2c642eb5\",\"_SYSTEMD_SLICE\": \"system.slice\",\"SYSLOG_IDENTIFIER\": \"kubelet\",\"_CAP_EFFECTIVE\": \"3fffffffff\",\"_STREAM_ID\": \"6aba8f44b68e4718a13999c63d142e28\",\"_CMDLINE\": \"/home/kubernetes/bin/kubelet --v=2 --cloud-provider=gce --experimental-check-node-capabilities-before-mount=true --experimental-mounter-path=/home/kubernetes/containerized_mounter/mounter --cert-dir=/var/lib/kubelet/pki/ --cni-bin-dir=/home/kubernetes/bin --kubeconfig=/var/lib/kubelet/kubeconfig --image-pull-progress-deadline=5m --experimental-kernel-memcg-notification=true --max-pods=110 --non-masquerade-cidr=0.0.0.0/0 --network-plugin=kubenet --node-labels=beta.kubernetes.io/fluentd-ds-ready=true,cloud.google.com/gke-nodepool=default-pool,cloud.google.com/gke-os-distribution=cos --volume-plugin-dir=/home/kubernetes/flexvolume --bootstrap-kubeconfig=/var/lib/kubelet/bootstrap-kubeconfig --node-status-max-images=25 --registry-qps=10 --registry-burst=20 --config /home/kubernetes/kubelet-config.yaml --pod-sysctls=net.core.somaxconn=1024,net.ipv4.conf.all.accept_redirects=0,net.ipv4.conf.all.forwarding=1,net.ipv4.conf.all.route_localnet=1,net.ipv4.conf.default.forwarding=1,net.ipv4.ip_forward=1,net.ipv4.tcp_fin_timeout=60,net.ipv4.tcp_keepalive_intvl=75,net.ipv4.tcp_keepalive_probes=9,net.ipv4.tcp_keepalive_time=7200,net.ipv4.tcp_rmem=4096 87380 6291456,net.ipv4.tcp_syn_retries=6,net.ipv4.tcp_tw_reuse=0,net.ipv4.tcp_wmem=4096 16384 4194304,net.ipv4.udp_rmem_min=4096,net.ipv4.udp_wmem_min=4096,net.ipv6.conf.default.accept_ra=0,net.netfilter.nf_conntrack_generic_timeout=600,net.netfilter.nf_conntrack_tcp_timeout_close_wait=3600,net.netfilter.nf_conntrack_tcp_timeout_established=86400\",
                             \"_PID\": \"1243\",\"MESSAGE\": \"I0415 06:18:20.943836    1243 kubelet_getters.go:177] status for pod kube-proxy-gke-k8s-demo-default-pool-8eece21e-z6er updated to {Running [{Initialized True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:43 +0000 UTC  } {Ready True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:44 +0000 UTC  } {ContainersReady True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:44 +0000 UTC  } {PodScheduled True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:43 +0000 UTC  }]    10.128.15.198 10.128.15.198 2021-01-21 17:17:43 +0000 UTC [] [{kube-proxy {nil &ContainerStateRunning{StartedAt:2021-01-21 17:17:44 +0000 UTC,} nil} {nil nil nil} true 0 gke.gcr.io/kube-proxy:v1.15.12-gke.6002 docker://sha256:d97a2b9779ce0e53fc2ae82dc68a6ca97c1e8cbe111f99814715b730c28523e7 docker://0a8d009d27318e01ee97899e70b2a9d9de27d62a7baf781574f648a2cb5e9aa1}] Burstable}\"",
        "jsonPayload" => {
            "PRIORITY" => "6",
            "_HOSTNAME" => "gke-k8s-demo-default-pool-8eece21e-z6er",
            "_GID" => "0",
            "_BOOT_ID" => "fa612042f606422a80b4c64c2c642eb5",
            "_SYSTEMD_SLICE" => "system.slice",
            "SYSLOG_IDENTIFIER" => "kubelet",
            "_CAP_EFFECTIVE" => "3fffffffff",
            "_STREAM_ID" => "6aba8f44b68e4718a13999c63d142e28",
            "_CMDLINE" => "/home/kubernetes/bin/kubelet --v=2 --cloud-provider=gce --experimental-check-node-capabilities-before-mount=true --experimental-mounter-path=/home/kubernetes/containerized_mounter/mounter --cert-dir=/var/lib/kubelet/pki/ --cni-bin-dir=/home/kubernetes/bin --kubeconfig=/var/lib/kubelet/kubeconfig --image-pull-progress-deadline=5m --experimental-kernel-memcg-notification=true --max-pods=110 --non-masquerade-cidr=0.0.0.0/0 --network-plugin=kubenet --node-labels=beta.kubernetes.io/fluentd-ds-ready=true,cloud.google.com/gke-nodepool=default-pool,cloud.google.com/gke-os-distribution=cos --volume-plugin-dir=/home/kubernetes/flexvolume --bootstrap-kubeconfig=/var/lib/kubelet/bootstrap-kubeconfig --node-status-max-images=25 --registry-qps=10 --registry-burst=20 --config /home/kubernetes/kubelet-config.yaml --pod-sysctls=net.core.somaxconn=1024,net.ipv4.conf.all.accept_redirects=0,net.ipv4.conf.all.forwarding=1,net.ipv4.conf.all.route_localnet=1,net.ipv4.conf.default.forwarding=1,net.ipv4.ip_forward=1,net.ipv4.tcp_fin_timeout=60,net.ipv4.tcp_keepalive_intvl=75,net.ipv4.tcp_keepalive_probes=9,net.ipv4.tcp_keepalive_time=7200,net.ipv4.tcp_rmem=4096 87380 6291456,net.ipv4.tcp_syn_retries=6,net.ipv4.tcp_tw_reuse=0,net.ipv4.tcp_wmem=4096 16384 4194304,net.ipv4.udp_rmem_min=4096,net.ipv4.udp_wmem_min=4096,net.ipv6.conf.default.accept_ra=0,net.netfilter.nf_conntrack_generic_timeout=600,net.netfilter.nf_conntrack_tcp_timeout_close_wait=3600,net.netfilter.nf_conntrack_tcp_timeout_established=86400",
            "_PID" => "1243",
            "MESSAGE" => "I0415 06:18:20.943836    1243 kubelet_getters.go:177] status for pod kube-proxy-gke-k8s-demo-default-pool-8eece21e-z6er updated to {Running [{Initialized True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:43 +0000 UTC  } {Ready True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:44 +0000 UTC  } {ContainersReady True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:44 +0000 UTC  } {PodScheduled True 0001-01-01 00:00:00 +0000 UTC 2021-01-21 17:17:43 +0000 UTC  }]    10.128.15.198 10.128.15.198 2021-01-21 17:17:43 +0000 UTC [] [{kube-proxy {nil &ContainerStateRunning{StartedAt:2021-01-21 17:17:44 +0000 UTC,} nil} {nil nil nil} true 0 gke.gcr.io/kube-proxy:v1.15.12-gke.6002 docker://sha256:d97a2b9779ce0e53fc2ae82dc68a6ca97c1e8cbe111f99814715b730c28523e7 docker://0a8d009d27318e01ee97899e70b2a9d9de27d62a7baf781574f648a2cb5e9aa1}] Burstable}"
            },
        "resource" => {
            "type" => "gce_instance",
            "labels" => {
                "project_id" => "development-198123",
                "zone" => "us-central1-a",
                "instance_id" => "1437928523886234104"
                }
            },
        "timestamp" => "2021-04-15T06:18:20.943939Z",
        "logName" => "projects/development-198123/logs/kubelet",
        "receiveTimestamp" => "2021-04-15T06:18:26.830083776Z"
        },
        {
        "_lm.resourceId" => {
            "system.gcp.resourcename" =>"gke-standard-cluster-1-a-default-pool-96c859da-e9e7"
            },
        "message" =>"BaudRate/Actual (115200/115200) = 100%\r\n",
            "insertId" => "181",
            "resource" => {
                "type" => "gce_instance"
                },
        "timestamp" => "2021-04-28T13:08:27.808628469Z",
        "labels" => {
            "compute.googleapis.com/resource_name" => "gke-standard-cluster-1-a-default-pool-96c859da-e9e7"
        },
        "logName" => "projects/development-198123/logs/serialconsole.googleapis.com%2Fserial_port_1_output",
        "receiveTimestamp" => "2021-04-28T13:08:29.563802575Z"
        }]
      actual = filter(messages)
      actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
    end

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
                "system.gcp.resourcename" =>"projects/logicmonitor.com:api-project-650342240768/locations/asia-south1/functions/testfunctionlogsToPubsub"
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
                "system.gcp.resourceid" =>"development-198123:rwaver-sql-1"
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
            "receiveTimestamp" => "2021-04-27T06:44:51.904218698Z",
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
             }
            },
            {
            "_lm.resourceId" => {},
            "message" =>"{\"@type\": \"type.googleapis.com/google.cloud.audit.AuditLog\",\"status\": {\"message\": \"Instance Group Manager 'projects/272452262635/zones/us-central1-c/instanceGroupManagers/aef-backend-20180822t164014-00' initiated recreateInstance on instance 'projects/272452262635/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j'. Reason: instance's current action is VERIFYING, but health status is TIMEOUT.\"},\"authenticationInfo\": {\"principalEmail\": \"system@google.com\"},\"serviceName\": \"compute.googleapis.com\",\"methodName\": \"compute.instances.repair.recreateInstance\",\"resourceName\": \"projects/development-198123/zones/us-central1-c/instances/aef-backend-20180822t164014-b75j\",\"request\": {\"@type\": \"type.googleapis.com/compute.instances.repair.recreateInstance\"}}",
            "resource" => {
                "type" => "gce_instance",
                "labels" => {
                    "project_id" => "development-198123"
                    }
                },
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
        actual.each_with_index { |val, index| assert_equal(expected[index]["_lm.resourceId"], actual[index]["_lm.resourceId"]) }
        end
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