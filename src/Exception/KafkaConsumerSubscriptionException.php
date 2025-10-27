<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

use Exception;

class KafkaConsumerSubscriptionException extends Exception
{
    public const MIXED_SUBSCRIPTION_EXCEPTION_MESSAGE
        = 'Dont mix subscriptions and assignments (with and without partitions defined).';
}
