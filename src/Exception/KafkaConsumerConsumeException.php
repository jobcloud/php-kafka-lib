<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

use Exception;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use Throwable;

class KafkaConsumerConsumeException extends Exception
{
    public const NOT_SUBSCRIBED_EXCEPTION_MESSAGE = 'This consumer is currently not subscribed';

    public function __construct(
        string $message = '',
        int $code = 0,
        private ?KafkaConsumerMessageInterface $kafkaMessage = null,
        ?Throwable $previous = null,
    ) {
        parent::__construct($message, $code, $previous);
    }

    public function getKafkaMessage(): ?KafkaConsumerMessageInterface
    {
        return $this->kafkaMessage;
    }
}
