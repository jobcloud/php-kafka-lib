<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Encoder;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;

final class NullEncoder implements EncoderInterface
{

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        return $producerMessage;
    }
}
