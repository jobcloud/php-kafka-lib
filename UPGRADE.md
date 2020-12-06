# Upgrade to 1.0

## Avro encoding / decoding
Has been removed as a fixed dependency. If you rely on it you need to run  
the following in your project:
```
composer require flix-tech/avro-serde-php "~1.4"
```

## Producer improvements
Producer used to poll all events after `produce`, now per default only one  
non-blocking poll call will be triggered after `produce`.  
This improvement was made so higher throughput can be achieved if needed.  
This affects the following classes:
- KafkaProducer
    - Added `syncProduce` - waits indefinitely for an event to be polled
    - Added `poll` - gives you the ability to poll, especially useful if you disable auto poll for `produce`
    - Added `pollUntilQueueSizeReached` - polls until the poll queue has reached a certain size
    - Changed `produce`, changed behaviour (see above), has new optional parameters `$autoPoll` and `$pollTimeoutMs`

To achieve the previous default behaviour change (e.g. suited for REST API applications which trigger a message on call and are not using long running processes like Swoole):
```
$producer->produce($message);
```
to
```
$producer->produce($message, false);
$producer->pollUntilQueueSizeReached(); // defaults should work fine
```

## Possibility to decode message later (Consumer)
Consume has now a second optional parameter `consume(int $timeoutMs = 10000, bool $autoDecode = true)`.  
If set to false, you must decode your message later using `$consumer->decodeMessage($message)`.  
For high throughput, you don't need to decode immediately, some decisions can be made  
relying on the message headers alone. This helps to leverage that.

## Remove timout from builder / configuration, added timeout parameter to functions (Consumer / Producer)
You were able to set timeout in the builder, but some methods  
still had `timeout` as parameter. To avoid confusion, every method  
needing a timeout, will now have it as a parameter with a sane default value.  
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
    
## Possibility to avro encode / decode both key and body of a message
The previous default behaviour was to only encode the body of a message.  
Now you are also able to encode / decode message keys.  
This affects the following classes:  
- AvroSchemaRegistry
    - Removed `addSchemaMappingForTopic`
    - Removed `getSchemaForTopic`
    - Added `addBodySchemaMappingForTopic`
    - Added `addKeySchemaMappingForTopic`
    - Added `getBodySchemaForTopic`
    - Added `getKeySchemaForTopic`
    - Behaviour change: Trying to get a schema for which no mapping was registered will throw `AvroSchemaRegistryException`
    - `getTopicSchemaMapping` will still return an array with mappings but with an additional first key `body` or `key` for the type of the schema  
- KafkaMessageInterface
    - type of `key` is now mixed

## Default error callback
The default error callback now only throws exceptions for fatal errors.  
Other errors will be retried by librdkafka and are only informational.
