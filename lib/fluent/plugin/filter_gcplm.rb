require 'fluent/plugin/filter'

module Fluent::Plugin
  class GCPLMFilter < Filter
    Fluent::Plugin.register_filter('gcplm', self)

    METADATA_KEYS_TO_RENAME = {"trace" => "trace_id", "spanId" => "span_id","resource.type" => "_type"}.freeze
    STATIC_METADATA = {"_integration" => "gcp"}

    config_param :metadata_keys, :array, default: ["severity", "logName", "labels", "resource.type", "resource.labels", "httpRequest", "trace", "spanId" ], value_type: :string
    config_param :use_default_severity, :bool, default: false 

    def configure(conf)
      super
      # Do the usual configuration here
    end

# This method is called when starting.
    # Open sockets or files here.
    def start
      @final_metadata_keys = Hash.new
      if @metadata_keys
        @metadata_keys.each do | nested_key |
          @final_metadata_keys[nested_key] = nested_key.to_s.split('.') 
        end  
      end  
      super
    end

    # This method is called when shutting down.
    # Shutdown the thread and close sockets or files here.
    def shutdown
      super
    end

    def filter(tag, time, record)
      message = String.new
      # The type of service in GCP
      resourceType = record.dig("resource", "type")
      resourceMap = Hash.new
      # The id of project in GCP
      project_id = record.dig("resource", "labels", "project_id")
      # The region where the service is running in GCP
      region = record.dig("resource", "labels", "region")
      filteredRecord = Hash.new

      case
      # Capturing json and text payloads as message field
      when record['textPayload']
        message = record['textPayload']
      when record['jsonPayload']
        message = record['jsonPayload'].to_json
      # Capturing the protoPayload when we receive an audit log
      when record['protoPayload']
        message = record['protoPayload'].to_json
      # for cloudRun we have request logs getting logged when a request is made to the service which has the statusCode, request type etc in httprequest field in the log
      when record['httpRequest'] && resourceType == 'cloud_run_revision'
        message = record['httpRequest'].to_json
      else
        message = nil
      end

      # Mapping the '_lm.resourceId' to the specific resourceId or resourceName depending on the type of service in GCP
      case resourceType
      when 'gce_instance'
        if (record.dig("resource", "labels", "instance_id"))
            resourceMap = {"system.gcp.resourceid" => record.dig("resource", "labels", "instance_id"), "system.cloud.category" => 'GCP/ComputeEngine'}
        elsif (record.dig("labels", "compute.googleapis.com/resource_name"))
            resourceMap = {"system.gcp.resourcename" => record.dig("labels", "compute.googleapis.com/resource_name"), "system.cloud.category" => 'GCP/ComputeEngine'}
        end
      when 'cloud_function'
        resourceMap = {"system.gcp.resourcename" => "projects/" + project_id + "/locations/" + region +"/functions/" + record.dig("resource", "labels", "function_name"), "system.cloud.category" => 'GCP/CloudFunction'}
      when 'cloudsql_database'
        resourceMap = {"system.gcp.resourceid" => record.dig("resource", "labels", "database_id"), "system.cloud.category" => 'GCP/CloudSQL'}
      when 'cloud_run_revision'
        resourceMap = {"system.gcp.resourcename" => record.dig("resource", "labels", "service_name"), "system.cloud.category" => 'GCP/CloudRun'}
      when 'cloud_composer_environment'
        resourceMap = {"system.gcp.resourcename" => "projects/" + project_id + "/locations/" + record.dig("resource", "labels", "location") + "/environments/" + record.dig("resource", "labels", "environment_name"), "system.cloud.category" => 'GCP/CloudComposer'}
      end

      if(record.key?("protoPayload") && record.dig('protoPayload', '@type') == "type.googleapis.com/google.cloud.audit.AuditLog")
        resourceMap = {"system.cloud.category" => 'GCP/LMAccount'}
      end


      resourceMap['system.gcp.projectId'] = project_id if !resourceMap.empty?
      # Creating a new record which is further sent to LM
      filteredRecord['message'] = message
      filteredRecord['_lm.resourceId'] = resourceMap
      filteredRecord['timestamp'] = record['timestamp']

      # Add metadata
      if @final_metadata_keys 
        @final_metadata_keys.each do | key, value |
          final_key = METADATA_KEYS_TO_RENAME[key] ? METADATA_KEYS_TO_RENAME[key] : key
          nestedVal = record
          value.each { |x| nestedVal = nestedVal[x] }
          if nestedVal != nil
            filteredRecord[final_key] = nestedVal
          end  
        end  
      end  

      # Add static metadata
      STATIC_METADATA.each { | key, value| filteredRecord[key] = value }

      # Add default severity if does not exist 
      if !filteredRecord["severity"] and @use_default_severity
        filteredRecord["severity"] = "DEFAULT"
      end   

      filteredRecord
    end
  end
end