# lm-filter-gcp-fluentd
This filter plugin is provided for mapping the logs coming from GCP pubsub to '_lm.resourceId' which is used for sending GCP logs to LM https://www.logicmonitor.com/support/lm-logs/setting-up-gcp-logs-ingestion. The plugin maps the following services to '_lm.resourceId' in LM :
* Google Compute Engine
* Google Cloud Functions
* Google Cloud SQL
* Google Audit Logs
* Google Cloud Composer
* Google Cloud Run

