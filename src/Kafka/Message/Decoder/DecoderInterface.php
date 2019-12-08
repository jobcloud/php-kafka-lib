<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Decoder;

use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;

interface DecoderInterface
{
    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface;
}
