# lm-filter-gcp-fluentd
This filter plugin is provided for mapping the logs coming from GCP pubsub to '_lm.resourceId' which is used for sending GCP logs to LM https://www.logicmonitor.com/support/lm-logs/setting-up-gcp-logs-ingestion. The plugin maps the following services to '_lm.resourceId' in LM :
* Google Compute Engine
* Google Cloud Functions
* Google Cloud SQL
* Google Audit Logs
* Google Cloud Composer
* Google Cloud Run

## LogicMonitor properties

| Property | Description |
| --- | --- |
| `metadata_keys` | Array of keys to be added as metadata. Filter will look for these keys in fluentd record and extract those if exist. in case of nested json, whole sub json would be added. default ` ["severity", "logName", "labels", "resource.type", "resource.labels", "httpRequest", "trace", "spanId" ] ` . few keys will be renamed as part of metadata standardization in Logicmonitor : ` {"logName" => "event_id", "trace" => "trace_id", "spanId" => "span_id","resource.type" => "_type"}`|
| `use_default_severity` | When `true`, and log record does not have severirty, severity=`DEFAULT` would be added to log. default `false`.  |
