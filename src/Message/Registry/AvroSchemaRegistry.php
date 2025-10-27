<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Registry;

use AvroSchema;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Kafka\Exception\AvroSchemaRegistryException;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;

final class AvroSchemaRegistry implements AvroSchemaRegistryInterface
{
    /** @var array<string, KafkaAvroSchemaInterface[]> */
    private array $schemaMapping = [
        self::BODY_IDX => [],
        self::KEY_IDX => [],
    ];

    public function __construct(private Registry $registry)
    {
    }

    public function addBodySchemaMappingForTopic(string $topicName, KafkaAvroSchemaInterface $avroSchema): void
    {
        $this->schemaMapping[self::BODY_IDX][$topicName] = $avroSchema;
    }

    public function addKeySchemaMappingForTopic(string $topicName, KafkaAvroSchemaInterface $avroSchema): void
    {
        $this->schemaMapping[self::KEY_IDX][$topicName] = $avroSchema;
    }

    /**
     * @throws SchemaRegistryException
     */
    public function getBodySchemaForTopic(string $topicName): KafkaAvroSchemaInterface
    {
        return $this->getSchemaForTopicAndType($topicName, self::BODY_IDX);
    }

    /**
     * @throws SchemaRegistryException
     */
    public function getKeySchemaForTopic(string $topicName): KafkaAvroSchemaInterface
    {
        return $this->getSchemaForTopicAndType($topicName, self::KEY_IDX);
    }

    public function hasBodySchemaForTopic(string $topicName): bool
    {
        return isset($this->schemaMapping[self::BODY_IDX][$topicName]);
    }

    public function hasKeySchemaForTopic(string $topicName): bool
    {
        return isset($this->schemaMapping[self::KEY_IDX][$topicName]);
    }

    /**
     * @throws SchemaRegistryException|AvroSchemaRegistryException
     */
    private function getSchemaForTopicAndType(string $topicName, string $type): KafkaAvroSchemaInterface
    {
        if (false === isset($this->schemaMapping[$type][$topicName])) {
            throw new AvroSchemaRegistryException(
                sprintf(
                    AvroSchemaRegistryException::SCHEMA_MAPPING_NOT_FOUND,
                    $topicName,
                    $type
                )
            );
        }

        $avroSchema = $this->schemaMapping[$type][$topicName];

        if (null !== $avroSchema->getDefinition()) {
            return $avroSchema;
        }

        $avroSchema->setDefinition($this->getSchemaDefinition($avroSchema));
        $this->schemaMapping[$type][$topicName] = $avroSchema;

        return $avroSchema;
    }

    /**
     * @throws SchemaRegistryException
     */
    private function getSchemaDefinition(KafkaAvroSchemaInterface $avroSchema): AvroSchema
    {
        if (KafkaAvroSchemaInterface::LATEST_VERSION === $avroSchema->getVersion()) {
            return $this->registry->latestVersion($avroSchema->getName());
        }

        return $this->registry->schemaForSubjectAndVersion($avroSchema->getName(), $avroSchema->getVersion());
    }

    public function getTopicSchemaMapping(): array
    {
        return $this->schemaMapping;
    }
}
