<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

class KafkaProducerTransactionAbortException extends \Exception
{
    public const TRANSACTION_REQUIRES_ABORT_EXCEPTION_MESSAGE =
        'Produce failed. You need to abort your current transaction and start a new one (%s)';
}
