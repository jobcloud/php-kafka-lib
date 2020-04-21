# Upgrade to 1.0
## Remove timout from builder / configuration, added to functions
You were able to set timeout in the builder, but some methods  
still had timeout as parameter. To avoid confusion, every method  
needing a timeout will now have it as a parameter with a sane default value.  
This affects the following classes:  
- KafkaConfiguration
    - Removed timeout parameter from `__construct()`  
    - Removed `getTimeout()`
- KafkaConsumerBuilder / KafkaConsumerBuilderInterface
    - Removed `withTimeout()`
- KafkaProducerBuilder / KafkaProducerBuilderInterface
    - Removed `withPollTimeout()`
- KafkaHighLevelConsumer / KafkaLowLevelConsumer / KafkaConsumerInterface
    - Added `$timeoutMs` to `consume()`

## Default error callback
The default error callback now only throws exceptions for  
fatal errors. Other errors will be retried by librdkafka  
and are only informational.