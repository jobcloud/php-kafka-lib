<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Decoder;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;

interface DecoderInterface
{
    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     * @throws SchemaRegistryException
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface;
}
