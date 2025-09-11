<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Encoder;

use AvroSchema;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Kafka\Exception\AvroEncoderException;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;

final class AvroEncoder implements AvroEncoderInterface
{
    public function __construct(
        private AvroSchemaRegistryInterface $registry,
        private RecordSerializer $recordSerializer
    ) {
    }

    /**
     * @throws SchemaRegistryException
     * @throws AvroEncoderException
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        $producerMessage = $this->encodeBody($producerMessage);

        return $this->encodeKey($producerMessage);
    }

    /**
     * @throws SchemaRegistryException
     */
    private function encodeBody(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        $topicName = $producerMessage->getTopicName();
        $body = $producerMessage->getBody();

        if (null === $body) {
            return $producerMessage;
        }

        if (false === $this->registry->hasBodySchemaForTopic($topicName)) {
            return $producerMessage;
        }

        $avroSchema = $this->registry->getBodySchemaForTopic($topicName);

        $encodedBody = $this->recordSerializer->encodeRecord(
            $avroSchema->getName(),
            $this->getAvroSchemaDefinition($avroSchema),
            $body
        );

        return $producerMessage->withBody($encodedBody);
    }

    /**
     * @throws SchemaRegistryException
     */
    private function encodeKey(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        $topicName = $producerMessage->getTopicName();
        $key = $producerMessage->getKey();

        if (null === $key) {
            return $producerMessage;
        }

        if (false === $this->registry->hasKeySchemaForTopic($topicName)) {
            return $producerMessage;
        }

        $avroSchema = $this->registry->getKeySchemaForTopic($topicName);

        $encodedKey = $this->recordSerializer->encodeRecord(
            $avroSchema->getName(),
            $this->getAvroSchemaDefinition($avroSchema),
            $key
        );

        return $producerMessage->withKey($encodedKey);
    }

    private function getAvroSchemaDefinition(KafkaAvroSchemaInterface $avroSchema): AvroSchema
    {
        $schemaDefinition = $avroSchema->getDefinition();

        if (null === $schemaDefinition) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::UNABLE_TO_LOAD_DEFINITION_MESSAGE,
                    $avroSchema->getName()
                )
            );
        }

        return $schemaDefinition;
    }

    public function getRegistry(): AvroSchemaRegistryInterface
    {
        return $this->registry;
    }
}
