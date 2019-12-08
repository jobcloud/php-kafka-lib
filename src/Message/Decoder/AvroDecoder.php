<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Decoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;

final class AvroDecoder implements AvroDecoderInterface
{

    /**
     * @var AvroSchemaRegistryInterface
     */
    private $registry;

    /**
     * @var RecordSerializer
     */
    private $recordSerializer;

    /**
     * @param AvroSchemaRegistryInterface $registry
     * @param RecordSerializer            $recordSerializer
     */
    public function __construct(AvroSchemaRegistryInterface $registry, RecordSerializer $recordSerializer)
    {
        $this->recordSerializer = $recordSerializer;
        $this->registry = $registry;
    }

    /**
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     * @throws SchemaRegistryException
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        $schemaDefinition = null;

        if (null === $consumerMessage->getBody()) {
            return $consumerMessage;
        }

        $avroSchema = $this->registry->getSchemaForTopic($consumerMessage->getTopicName());

        if (true === $avroSchema instanceof KafkaAvroSchemaInterface) {
            $schemaDefinition = $avroSchema->getDefinition();
        }

        $body = $this->recordSerializer->decodeMessage($consumerMessage->getBody(), $schemaDefinition);

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

    /**
     * @return AvroSchemaRegistryInterface
     */
    public function getRegistry(): AvroSchemaRegistryInterface
    {
        return $this->registry;
    }
}
