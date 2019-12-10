<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

class KafkaRebalanceException extends \Exception
{
    public const REBALANCE_EXCEPTION_MESSAGE = 'Error during rebalance of consumer';
}
