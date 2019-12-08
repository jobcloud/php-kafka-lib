<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Exception;

use Jobcloud\Messaging\Consumer\ConsumerException;

class KafkaConsumerSubscriptionException extends ConsumerException
{
    public const MIXED_SUBSCRIPTION_EXCEPTION_MESSAGE
        = 'Dont mix subscriptions and assignments (with and without partitions defined).';
}
