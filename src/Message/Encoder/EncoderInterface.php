<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Encoder;

use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;

interface EncoderInterface
{
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface;
}
