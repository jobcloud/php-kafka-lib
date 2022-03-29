<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

class KafkaProducerTransactionFatalException extends \Exception
{
    public const FATAL_TRANSACTION_EXCEPTION_MESSAGE =
        'Produce failed with a fatal error. This producer instance cannot be used anymore (%s)';
}
