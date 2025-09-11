<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Decoder;

use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;

final class NullDecoder implements DecoderInterface
{
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        return $consumerMessage;
    }
}
