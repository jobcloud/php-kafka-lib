<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Encoder;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;

class JsonEncoder implements EncoderInterface
{

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        $body = json_encode($producerMessage->getBody(), JSON_THROW_ON_ERROR);

        return $producerMessage->withBody($body);
    }
}
