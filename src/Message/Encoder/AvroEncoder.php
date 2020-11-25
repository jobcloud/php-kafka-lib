<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Encoder;

use AvroSchema;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroEncodingException;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Avro\Validator\Exception\RecordRegistryException;
use Jobcloud\Avro\Validator\Exception\ValidatorException;
use Jobcloud\Avro\Validator\RecordRegistry;
use Jobcloud\Avro\Validator\Validator;
use Jobcloud\Kafka\Exception\AvroEncoderException;
use Jobcloud\Kafka\Exception\AvroValidatorException;
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
     * @throws AvroValidatorException
     * @throws SchemaRegistryException
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        $producerMessage = $this->encodeBody($producerMessage);

        return $this->encodeKey($producerMessage);
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     * @throws AvroValidatorException
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

        $encodedBody = $this->encodeRecord($avroSchema, $body, $topicName);

        return $producerMessage->withBody($encodedBody);
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     * @throws AvroValidatorException
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

        $encodedKey = $this->encodeRecord($avroSchema, $key, $topicName);

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
     * @param KafkaAvroSchemaInterface $avroSchema
     * @param mixed $data
     * @param string $topicName
     * @return string
     * @throws SchemaRegistryException
     * @throws AvroValidatorException
     */
    private function encodeRecord(KafkaAvroSchemaInterface $avroSchema, $data, string $topicName): string
    {
        try {
            $encodedData = $this->recordSerializer->encodeRecord(
                $avroSchema->getName(),
                $this->getAvroSchemaDefinition($avroSchema),
                $data
            );
        } catch (AvroEncodingException $exception) {
            if (class_exists(Validator::class)) {
                /** @var AvroSchema $schemaDefinition */
                $schemaDefinition = $avroSchema->getDefinition();
                $recordRegistry = RecordRegistry::fromSchema(json_encode($schemaDefinition->to_avro()));
                $validator = new Validator($recordRegistry);

                $validationErrors = $validator->validate(json_encode($data), $topicName);

                throw new AvroValidatorException((string) json_encode($validationErrors));
            }

            throw $exception;
        }

        return $encodedData;
    }
}
