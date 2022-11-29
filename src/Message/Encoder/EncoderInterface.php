<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Encoder;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Kafka\Exception\AvroEncoderException;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;

interface EncoderInterface
{
    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     * @throws SchemaRegistryException
     * @throws AvroEncoderException
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface;
}
