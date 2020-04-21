# Upgrade to 1.0
## Default error callback
The default error callback now only throws exceptions for  
fatal errors. Other errors will be retried by librdkafka  
and are only informational.