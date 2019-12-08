<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Decoder;

use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;

class JsonDecoder implements DecoderInterface
{

    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        $body = json_decode($consumerMessage->getBody(), true, 512, JSON_THROW_ON_ERROR);

        return new KafkaConsumerMessage(
            $consumerMessage->getTopicName(),
            $consumerMessage->getPartition(),
            $consumerMessage->getOffset(),
            $consumerMessage->getTimestamp(),
            $consumerMessage->getKey(),
            $body,
            $consumerMessage->getHeaders()
        );
    }
}
