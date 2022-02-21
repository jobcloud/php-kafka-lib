<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Encoder;

use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;

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
