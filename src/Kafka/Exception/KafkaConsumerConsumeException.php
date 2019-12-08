<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Exception;

use Jobcloud\Messaging\Consumer\ConsumerException;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;

class KafkaConsumerConsumeException extends ConsumerException
{

    public const NOT_SUBSCRIBED_EXCEPTION_MESSAGE = 'This consumer is currently not subscribed';

    /**
     * @var KafkaConsumerMessageInterface|null
     */
    private $kafkaMessage;

    /**
     * @param string                     $message
     * @param integer                    $code
     * @param KafkaConsumerMessageInterface|null $kafkaMessage
     * @param \Throwable|null            $previous
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        KafkaConsumerMessageInterface $kafkaMessage = null,
        \Throwable $previous = null
    ) {
        parent::__construct($message, $code, $previous);

        $this->kafkaMessage = $kafkaMessage;
    }

    /**
     * @return null|KafkaConsumerMessageInterface
     */
    public function getKafkaMessage(): ?KafkaConsumerMessageInterface
    {
        return $this->kafkaMessage;
    }
}
