<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Encoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Kafka\Exception\AvroEncoderException;
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
        $body = $this->encodeBody($producerMessage);
        $key = $this->encodeKey($producerMessage);

        return $producerMessage->withBody($body)->withKey($key);
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

        if (null === $avroSchema = $this->registry->getBodySchemaForTopic($producerMessage->getTopicName())) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::NO_BODY_SCHEMA_FOR_TOPIC_MESSAGE,
                    $producerMessage->getTopicName()
                )
            );
        }

        if (null === $avroSchema->getDefinition()) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::UNABLE_TO_LOAD_DEFINITION_MESSAGE,
                    $avroSchema->getName()
                )
            );
        }

        return $this->recordSerializer->encodeRecord(
            $avroSchema->getName(),
            $avroSchema->getDefinition(),
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

        if (null === $avroSchema = $this->registry->getKeySchemaForTopic($producerMessage->getTopicName())) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::NO_KEY_SCHEMA_FOR_TOPIC_MESSAGE,
                    $producerMessage->getTopicName()
                )
            );
        }

        if (null === $avroSchema->getDefinition()) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::UNABLE_TO_LOAD_DEFINITION_MESSAGE,
                    $avroSchema->getName()
                )
            );
        }

        return $this->recordSerializer->encodeRecord(
            $avroSchema->getName(),
            $avroSchema->getDefinition(),
            $producerMessage->getKey()
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
