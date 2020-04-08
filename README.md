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
- php: ^7.3
- ext-rdkafka: ^4.0.0

## Installation
```composer require jobcloud/php-kafka-lib "~0.1"```

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
$registry->addSchemaMappingForTopic(
    'test-topic',
    new KafkaAvroSchema('schemaName' /*, int $version, AvroSchema $definition */)
);

$encoder = new AvroEncoder($registry, $recordSerializer);

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
    ->withTimeout(120 * 10000)
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
    ->withTimeout(120 * 10000)
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
$registry->addSchemaMappingForTopic(
    'test-topic',
    new KafkaAvroSchema('someSchema' , 9 /* , AvroSchema $definition */)
);

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
    ->withTimeout(120 * 10000)
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
