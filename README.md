# php-kafka-lib

[![CircleCI](https://circleci.com/gh/jobcloud/php-kafka-lib.svg?style=shield)](https://circleci.com/gh/jobcloud/php-kafka-lib) 
[![Maintainability](https://api.codeclimate.com/v1/badges/beae5fe991d080cbad8c/maintainability)](https://codeclimate.com/github/jobcloud/php-kafka-lib/maintainability) 
[![Test Coverage](https://api.codeclimate.com/v1/badges/beae5fe991d080cbad8c/test_coverage)](https://codeclimate.com/github/jobcloud/php-kafka-lib/test_coverage) 
[![Latest Stable Version](https://poser.pugx.org/jobcloud/php-kafka-lib/v/stable)](https://packagist.org/packages/jobcloud/php-kafka-lib) 
[![Latest Unstable Version](https://poser.pugx.org/jobcloud/php-kafka-lib/v/unstable)](https://packagist.org/packages/jobcloud/php-kafka-lib) 

## Description
This is a library that makes it easier to use Kafka in your PHP project.  

This library relies on [arnaud-lb/php-rdkafka](https://github.com/arnaud-lb/php-rdkafka)  
Avro support relies on [flix-tech/avro-serde-php](https://github.com/flix-tech/avro-serde-php)  
The [documentation](https://arnaud.le-blanc.net/php-rdkafka/phpdoc/book.rdkafka.html) of the php extension,  
can help out to understand the internals of this library.

## Requirements
- php: ^7.3|^8.0
- ext-rdkafka: >=4.0.0
- librdkafka: >=0.11.6 (if you use `<librdkafka:1.x` please define your own error callback)

:warning: To use the transactional producer you'll need:
- ext-rdkafka: >=4.1.0
- librdkafka: >=1.4

## Installation
```
composer require jobcloud/php-kafka-lib "~1.0"
```

### Enable Avro support
If you need Avro support, run:
```
composer require flix-tech/avro-serde-php "~1.4"
```

## Usage

### Producer

#### Kafka

##### Simple example
```php
<?php

use Jobcloud\Kafka\Message\KafkaProducerMessage;
use Jobcloud\Kafka\Producer\KafkaProducerBuilder;

$producer = KafkaProducerBuilder::create()
    ->withAdditionalBroker('localhost:9092')
    ->build();

$message = KafkaProducerMessage::create('test-topic', 0)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test message payload')
            ->withHeaders([ 'key' => 'value' ]);

$producer->produce($message);

// Shutdown producer, flush messages that are in queue. Give up after 20s
$result = $producer->flush(20000);
```

##### Transactional producer (needs >=php-rdkafka:4.1 and >=librdkafka:1.4)
```php
<?php

use Jobcloud\Kafka\Message\KafkaProducerMessage;
use Jobcloud\Kafka\Producer\KafkaProducerBuilder;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionRetryException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionAbortException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionFatalException;

$producer = KafkaProducerBuilder::create()
    ->withAdditionalBroker('localhost:9092')
    ->build();

$message = KafkaProducerMessage::create('test-topic', 0)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test message payload')
            ->withHeaders([ 'key' => 'value' ]);
try {
    $producer->beginTransaction(10000);
    $producer->produce($message);
    $producer->commitTransaction(10000);
} catch (KafkaProducerTransactionRetryException $e) {
    // something went wrong but you can retry the failed call (either beginTransaction or commitTransaction)
} catch (KafkaProducerTransactionAbortException $e) {
    // you need to call $producer->abortTransaction(10000); and try again
} catch (KafkaProducerTransactionFatalException $e) {
    // something went very wrong, re-create your producer, otherwise you could jeopardize the idempotency guarantees
}

// Shutdown producer, flush messages that are in queue. Give up after 20s
$result = $producer->flush(20000);
```

##### Avro Producer
To create an avro prodcuer add the avro encoder.  

```php
<?php

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use Jobcloud\Kafka\Message\KafkaProducerMessage;
use Jobcloud\Kafka\Message\Encoder\AvroEncoder;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistry;
use Jobcloud\Kafka\Producer\KafkaProducerBuilder;
use Jobcloud\Kafka\Message\KafkaAvroSchema;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use GuzzleHttp\Client;

$cachedRegistry = new CachedRegistry(
    new BlockingRegistry(
        new PromisingRegistry(
            new Client(['base_uri' => 'jobcloud-kafka-schema-registry:9081'])
        )
    ),
    new AvroObjectCacheAdapter()
);

$registry = new AvroSchemaRegistry($cachedRegistry);
$recordSerializer = new RecordSerializer($cachedRegistry);

//if no version is defined, latest version will be used
//if no schema definition is defined, the appropriate version will be fetched form the registry
$registry->addBodySchemaMappingForTopic(
    'test-topic',
    new KafkaAvroSchema('bodySchemaName' /*, int $version, AvroSchema $definition */)
);
$registry->addKeySchemaMappingForTopic(
    'test-topic',
    new KafkaAvroSchema('keySchemaName' /*, int $version, AvroSchema $definition */)
);

// if you are only encoding key or value, you can pass that mode as additional third argument
// per default both key and body will get encoded
$encoder = new AvroEncoder($registry, $recordSerializer /*, AvroEncoderInterface::ENCODE_BODY */);

$producer = KafkaProducerBuilder::create()
    ->withAdditionalBroker('kafka:9092')
    ->withEncoder($encoder)
    ->build();

$schemaName = 'testSchema';
$version = 1;
$message = KafkaProducerMessage::create('test-topic', 0)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody(['name' => 'someName'])
            ->withHeaders([ 'key' => 'value' ]);

$producer->produce($message);

// Shutdown producer, flush messages that are in queue. Give up after 20s
$result = $producer->flush(20000);
```

**NOTE:** To improve producer latency you can install the `pcntl` extension.  
The php-kafka-lib already has code in place, similarly described here:  
https://github.com/arnaud-lb/php-rdkafka#performance--low-latency-settings

### Consumer

#### Kafka High Level

```php
<?php

use Jobcloud\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Kafka\Exception\KafkaConsumerTimeoutException;

$consumer = KafkaConsumerBuilder::create()
     ->withAdditionalConfig(
        [
            'compression.codec' => 'lz4',
            'auto.commit.interval.ms' => 500
        ]
    )
    ->withAdditionalBroker('kafka:9092')
    ->withConsumerGroup('testGroup')
    ->withAdditionalSubscription('test-topic')
    ->build();

$consumer->subscribe();

while (true) {
    try {
        $message = $consumer->consume();
        // your business logic
        $consumer->commit($message);
    } catch (KafkaConsumerTimeoutException $e) {
        //no messages were read in a given time
    } catch (KafkaConsumerEndOfPartitionException $e) {
        //only occurs if enable.partition.eof is true (default: false)
    } catch (KafkaConsumerConsumeException $e) {
        // Failed
    }
}
```

#### Kafka Low Level

```php
<?php

use Jobcloud\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Kafka\Exception\KafkaConsumerTimeoutException;

$consumer = KafkaConsumerBuilder::create()
     ->withAdditionalConfig(
        [
            'compression.codec' => 'lz4',
            'auto.commit.interval.ms' => 500
        ]
    )
    ->withAdditionalBroker('kafka:9092')
    ->withConsumerGroup('testGroup')
    ->withAdditionalSubscription('test-topic')
    ->withConsumerType(KafkaConsumerBuilder::CONSUMER_TYPE_LOW_LEVEL)
    ->build();

$consumer->subscribe();

while (true) {
    try {
        $message = $consumer->consume();
        // your business logic
        $consumer->commit($message);
    } catch (KafkaConsumerTimeoutException $e) {
        //no messages were read in a given time
    } catch (KafkaConsumerEndOfPartitionException $e) {
        //only occurs if enable.partition.eof is true (default: false)
    } catch (KafkaConsumerConsumeException $e) {
        // Failed
    } 
}
```

#### Avro Consumer
To create an avro consumer add the avro decoder.  

```php
<?php

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Kafka\Exception\KafkaConsumerTimeoutException;
use Jobcloud\Kafka\Message\Decoder\AvroDecoder;
use Jobcloud\Kafka\Message\KafkaAvroSchema;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistry;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use GuzzleHttp\Client;

$cachedRegistry = new CachedRegistry(
    new BlockingRegistry(
        new PromisingRegistry(
            new Client(['base_uri' => 'jobcloud-kafka-schema-registry:9081'])
        )
    ),
    new AvroObjectCacheAdapter()
);

$registry = new AvroSchemaRegistry($cachedRegistry);
$recordSerializer = new RecordSerializer($cachedRegistry);

//if no version is defined, latest version will be used
//if no schema definition is defined, the appropriate version will be fetched form the registry
$registry->addBodySchemaMappingForTopic(
    'test-topic',
    new KafkaAvroSchema('bodySchema' , 9 /* , AvroSchema $definition */)
);
$registry->addKeySchemaMappingForTopic(
    'test-topic',
    new KafkaAvroSchema('keySchema' , 9 /* , AvroSchema $definition */)
);

// If you are only encoding / decoding key or value, only register the schema(s) you need.
// It is advised against doing that though, some tools might not play
// nice if you don't fully encode your message
$decoder = new AvroDecoder($registry, $recordSerializer);

$consumer = KafkaConsumerBuilder::create()
     ->withAdditionalConfig(
        [
            'compression.codec' => 'lz4',
            'auto.commit.interval.ms' => 500
        ]
    )
    ->withDecoder($decoder)
    ->withAdditionalBroker('kafka:9092')
    ->withConsumerGroup('testGroup')
    ->withAdditionalSubscription('test-topic')
    ->build();

$consumer->subscribe();

while (true) {
    try {
        $message = $consumer->consume();
        // your business logic
        $consumer->commit($message);
    } catch (KafkaConsumerTimeoutException $e) {
        //no messages were read in a given time
    } catch (KafkaConsumerEndOfPartitionException $e) {
        //only occurs if enable.partition.eof is true (default: false)
    } catch (KafkaConsumerConsumeException $e) {
        // Failed
    } 
}
```

## Additional information
Replaces [messaging-lib](https://github.com/jobcloud/messaging-lib)  
Check [Migration.md](MIGRATION.md) for help to migrate.
