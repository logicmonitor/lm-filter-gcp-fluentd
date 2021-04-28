require 'fluent/plugin/filter'

module Fluent::Plugin
  class GCPLMFilter < Filter
    Fluent::Plugin.register_filter('gcplm', self)

    def configure(conf)
      super
      # Do the usual configuration here
    end

# This method is called when starting.
    # Open sockets or files here.
    def start
      super
    end

    # This method is called when shutting down.
    # Shutdown the thread and close sockets or files here.
    def shutdown
      super
    end

    def filter(tag, time, record)
      message = String.new
      resourceType = record.dig("resource","type")
      resourceMap = Hash.new
      project_id = record.dig("resource","labels", "project_id")
      region = record.dig("resource","labels", "region")

      case
      when record['textPayload']
        message = record['textPayload']
      when record['jsonPayload']
        message = record['jsonPayload'].to_json
      when record['protoPayload']
        message = record['protoPayload'].to_json
      else
        message = nil
      end

      case resourceType
      when 'gce_instance'
        if (record.dig("resource","labels", "instance_id"))
            resourceMap = {"system.gcp.resourceid" => record.dig("resource","labels", "instance_id")}
        elsif (record.dig("labels", "compute.googleapis.com/resource_name"))
            resourceMap = {"system.gcp.resourcename" => record.dig("labels", "compute.googleapis.com/resource_name")}
        end
      when 'cloud_function'
        resourceMap = {"system.gcp.resourcename" => "projects/" + project_id + "/locations/" + region +"/functions/" + record.dig("resource","labels", "function_name")}
      when 'cloudsql_database'
        resourceMap = {"system.gcp.resourceid" => record.dig("resource","labels", "database_id")}
      end

      if(record.key?("protoPayload") && record.dig('protoPayload','@type') == "type.googleapis.com/google.cloud.audit.AuditLog")
        resourceMap = {"system.gcp.projectId" => project_id, "system.cloud.category" => 'GCP/LMAccount'}
      end

      record['message'] = message
      record['_lm.resourceId'] = resourceMap
      record
    end
  end
end