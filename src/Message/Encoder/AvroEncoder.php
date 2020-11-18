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
     * @param AvroSchemaRegistryInterface $registry
     * @param RecordSerializer            $recordSerializer
     */
    public function __construct(
        AvroSchemaRegistryInterface $registry,
        RecordSerializer $recordSerializer
    ) {
        $this->recordSerializer = $recordSerializer;
        $this->registry = $registry;
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     * @throws SchemaRegistryException
     * @throws AvroEncoderException
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        try {
            $producerMessage = $this->encodeBody($producerMessage);
        } catch (\Exception $exception) {
            if (class_exists(\Jobcloud\Avro\Validator\Validator::class)) {
                $topicName = $producerMessage->getTopicName();
                $body = $producerMessage->getBody();

                $avroSchema = $this->registry->getBodySchemaForTopic($topicName);

                $validationErrors = $this->validateSchema(json_encode($avroSchema->getDefinition()->to_avro()), $body, $topicName);

                var_dump($validationErrors);
            }
        }

        return $this->encodeKey($producerMessage);
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
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
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
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

    /**
     * @return AvroSchemaRegistryInterface
     */
    public function getRegistry(): AvroSchemaRegistryInterface
    {
        return $this->registry;
    }

    /**
     * @param string $avroSchema
     * @param mixed $data
     * @param string $topicName
     * @return array<mixed>
     * @throws \Jobcloud\Avro\Validator\Exception\RecordRegistryException
     * @throws \Jobcloud\Avro\Validator\Exception\ValidatorException
     */
    private function validateSchema(string $avroSchema, $data, string $topicName): array
    {
        $recordRegistry = \Jobcloud\Avro\Validator\RecordRegistry::fromSchema((string) $avroSchema);
        $validator = new \Jobcloud\Avro\Validator\Validator($recordRegistry);

        return $validator->validate(json_encode($data), $topicName);
    }
}
