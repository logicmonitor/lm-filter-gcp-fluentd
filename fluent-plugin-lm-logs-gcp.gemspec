# Unless explicitly stated otherwise all files in this repository are licensed
# under the Apache License Version 2.0.
# This product includes software developed at LogicMonitor (https://www.logicmonitor.com).
# Copyright 2020 LogicMonitor, Inc.

Gem::Specification.new do |spec|
  spec.name                           = "fluent-plugin-lm-logs-gcp"
  spec.version                        = '1.0.1'
  spec.authors                        = ["LogicMonitor"]
  spec.email                          = "rubygems@logicmonitor.com"
  spec.summary                        = "LogicMonitor with GCP logs fluentd filter plugin"
  spec.description                    = "This filter plugin filters fluentd records in gcp to the configured LogicMonitor account."
  spec.homepage                       = "https://www.logicmonitor.com"
  spec.license                        = "Apache-2.0"

  spec.metadata["source_code_uri"]    = "https://github.com/logicmonitor/lm-logs-fluentd-gcp-filter"
  spec.metadata["documentation_uri"]  = "https://www.rubydoc.info/gems/lm-logs-fluentd-gcp-filter"

  spec.files         = [".gitignore", "Gemfile", "README.md", "LICENSE", "Rakefile", "fluent-plugin-lm-logs-gcp.gemspec", "lib/fluent/plugin/filter_gcplm.rb"]
  spec.require_paths = ["lib"]
  spec.required_ruby_version = '>= 2.0.0'

  spec.add_runtime_dependency "fluentd", [">= 1", "< 2"]
end