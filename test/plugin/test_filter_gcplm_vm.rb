require 'test/unit'
require "fluent/test"
require "fluent/test/helpers"
require 'fluent/test/driver/filter'
require "fluent/plugin/filter_gcplm"

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
            "system.gcp.resourceid" =>"1437928523886234104",
            "system.cloud.category" => 'GCP/ComputeEngine'
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
            "system.gcp.resourcename" =>"gke-standard-cluster-1-a-default-pool-96c859da-e9e7",
            "system.cloud.category" => 'GCP/ComputeEngine'
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
  end
end