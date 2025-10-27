<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Decoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;

final class AvroDecoder implements AvroDecoderInterface
{
    public function __construct(
        private AvroSchemaRegistryInterface $registry,
        private RecordSerializer $recordSerializer
    ) {
    }

    /**
     * @throws SchemaRegistryException
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        return new KafkaConsumerMessage(
            $consumerMessage->getTopicName(),
            $consumerMessage->getPartition(),
            $consumerMessage->getOffset(),
            $consumerMessage->getTimestamp(),
            $this->decodeKey($consumerMessage),
            $this->decodeBody($consumerMessage),
            $consumerMessage->getHeaders()
        );
    }

    /**
     * @throws SchemaRegistryException
     */
    private function decodeBody(KafkaConsumerMessageInterface $consumerMessage): mixed
    {
        $body = $consumerMessage->getBody();
        $topicName = $consumerMessage->getTopicName();

        if (null === $body) {
            return null;
        }

        if (false === $this->registry->hasBodySchemaForTopic($topicName)) {
            return $body;
        }

        $avroSchema = $this->registry->getBodySchemaForTopic($topicName);
        $schemaDefinition = $avroSchema->getDefinition();

        return $this->recordSerializer->decodeMessage($body, $schemaDefinition);
    }

    /**
     * @throws SchemaRegistryException
     */
    private function decodeKey(KafkaConsumerMessageInterface $consumerMessage): mixed
    {
        $key = $consumerMessage->getKey();
        $topicName = $consumerMessage->getTopicName();

        if (null === $key) {
            return null;
        }

        if (false === $this->registry->hasKeySchemaForTopic($topicName)) {
            return $key;
        }

        $avroSchema = $this->registry->getKeySchemaForTopic($topicName);
        $schemaDefinition = $avroSchema->getDefinition();

        return $this->recordSerializer->decodeMessage($key, $schemaDefinition);
    }

    public function getRegistry(): AvroSchemaRegistryInterface
    {
        return $this->registry;
    }
}
