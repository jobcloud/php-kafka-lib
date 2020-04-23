# Upgrade to 1.0

## Producer transactions
Added support for producer transactions, check [README.md](README.md) for more details.

## Producer control improvements
Producer used to poll all events after `produce`, now per default only one  
non-blocking poll call will be triggered after `produce`.
We have added the following new functions:
- `syncProduce` - waits indefinitely for an event to be polled
- `poll` - gives you the ability to poll, especially useful if you disable auto poll for `produce`
- `pollUntilQueueSizeReached` - polls until the poll queue has reached a certain size

## Possibility to decode message later (Consumer)
Default behaviour is the same.  
Consume has now an optional parameter `consume(bool $autoDecode = true)`.  
If set to false, you must decode your message later using `$consumer->decodeMessage($message)`.  
For high throughput, you don't need to decode immediately, some decisions can be made  
relying on the message headers alone. This helps to leverage that.

## Remove timout from builder / configuration, added timeout parameter to functions (Consumer / Producer)
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
    - Added `$timeoutMs` to `consume()`, default is 10s
    - Added `$timeoutMs` to `getMetadataForTopic()`, default is 10s
- KafkaProducer / KafkaProducerInterface
    - Added `pollTimeoutMs` to `produce()`, default is 10s
    - Added `$timeoutMs` to `getMetadataForTopic()`, default is 10s

## Default error callback
The default error callback now only throws exceptions for  
fatal errors. Other errors will be retried by librdkafka  
and are only informational.