<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Decoder;

use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;

final class NullDecoder implements DecoderInterface
{

    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        return $consumerMessage;
    }
}
