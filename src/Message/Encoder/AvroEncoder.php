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

    /**
     * @var AvroSchemaRegistryInterface
     */
    private $registry;

    /**
     * @var RecordSerializer
     */
    private $recordSerializer;

    /**
     * @var string
     */
    private $encodeMode;

    /**
     * @param AvroSchemaRegistryInterface $registry
     * @param RecordSerializer            $recordSerializer
     * @param string                      $encodeMode
     */
    public function __construct(
        AvroSchemaRegistryInterface $registry,
        RecordSerializer $recordSerializer,
        string $encodeMode = self::DECODE_ALL
    ) {
        $this->recordSerializer = $recordSerializer;
        $this->registry = $registry;
        $this->encodeMode = $encodeMode;
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     * @throws SchemaRegistryException
     * @throws AvroEncoderException
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        return $producerMessage
            ->withBody($this->encodeBody($producerMessage))
            ->withKey($this->encodeKey($producerMessage));
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return mixed
     * @throws SchemaRegistryException
     */
    private function encodeBody(KafkaProducerMessageInterface $producerMessage)
    {
        if (self::DECODE_KEY === $this->encodeMode) {
            return $producerMessage->getBody();
        }

        if (null === $producerMessage->getBody()) {
            return null;
        }

        $topicName = $producerMessage->getTopicName();
        $avroSchema = $this->getAvroSchema(
            $this->registry->getBodySchemaForTopic($topicName), $topicName, self::DECODE_KEY
        );


        return $this->recordSerializer->encodeRecord(
            $avroSchema->getName(),
            $this->getAvroSchemaDefinition($avroSchema),
            $producerMessage->getBody()
        );
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return string|null
     * @throws SchemaRegistryException
     */
    private function encodeKey(KafkaProducerMessageInterface $producerMessage): ?string
    {
        if (self::DECODE_BODY === $this->encodeMode) {
            return $producerMessage->getKey();
        }

        if (null === $producerMessage->getKey()) {
            return null;
        }

        $topicName = $producerMessage->getTopicName();
        $avroSchema = $this->getAvroSchema(
            $this->registry->getKeySchemaForTopic($topicName), $topicName, self::DECODE_KEY
        );

        return $this->recordSerializer->encodeRecord(
            $avroSchema->getName(),
            $this->getAvroSchemaDefinition($avroSchema),
            $producerMessage->getKey()
        );
    }

    /**
     * @param KafkaAvroSchemaInterface|null $avroSchema
     * @param string $topicName
     * @param string $type
     */
    private function getAvroSchema(
        ?KafkaAvroSchemaInterface $avroSchema,
        string $topicName,
        string $type
    ): KafkaAvroSchemaInterface {
        if (null === $avroSchema) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::NO_SCHEMA_FOR_TOPIC_MESSAGE,
                    $type,
                    $topicName
                )
            );
        }

        return $avroSchema;
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

    /**
     * @return AvroSchemaRegistryInterface
     */
    public function getRegistry(): AvroSchemaRegistryInterface
    {
        return $this->registry;
    }
}
