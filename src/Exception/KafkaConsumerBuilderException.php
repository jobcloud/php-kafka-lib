<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

class KafkaConsumerBuilderException extends \Exception
{
    public const NO_BROKER_EXCEPTION_MESSAGE = 'You need add at least one broker to connect to.';
    public const NO_TOPICS_EXCEPTION_MESSAGE = 'No topics defined to subscribe to.';
    public const UNSUPPORTED_CALLBACK_EXCEPTION_MESSAGE = 'The callback %s is not supported for %s';
    public const TOPIC_SUBSCRIPTION_OFFSET_WITHOUT_PARTITIONS = 'If you define an offset for a topic subscription, partition(s) must be defined as well';
}
