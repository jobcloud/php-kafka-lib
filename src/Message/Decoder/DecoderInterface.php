<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Decoder;

use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;

interface DecoderInterface
{
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface;
}
