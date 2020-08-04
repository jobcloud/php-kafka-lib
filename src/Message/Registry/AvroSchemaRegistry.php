<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Registry;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;

final class AvroSchemaRegistry implements AvroSchemaRegistryInterface
{
    /** @var string */
    private const BODY_IDX = 'body';

    /** @var string */
    private const KEY_IDX = 'key';

    /**
     * @var Registry
     */
    private $registry;

    /**
     * @var array<string, KafkaAvroSchemaInterface[]>
     */
    private $schemaMapping = [
        self::BODY_IDX => [],
        self::KEY_IDX => [],
    ];

    /**
     * AvroSchemaRegistry constructor.
     * @param Registry $registry
     */
    public function __construct(Registry $registry)
    {
        $this->registry = $registry;
    }

    /**
     * @param string                   $topicName
     * @param KafkaAvroSchemaInterface $avroSchema
     * @return void
     */
    public function addBodySchemaMappingForTopic(string $topicName, KafkaAvroSchemaInterface $avroSchema): void
    {
        $this->schemaMapping[self::BODY_IDX][$topicName] = $avroSchema;
    }

    /**
     * @param string                   $topicName
     * @param KafkaAvroSchemaInterface $avroSchema
     * @return void
     */
    public function addKeySchemaMappingForTopic(string $topicName, KafkaAvroSchemaInterface $avroSchema): void
    {
        $this->schemaMapping[self::KEY_IDX][$topicName] = $avroSchema;
    }

    /**
     * @param string $topicName
     * @return KafkaAvroSchemaInterface|null
     * @throws SchemaRegistryException
     */
    public function getBodySchemaForTopic(string $topicName): ?KafkaAvroSchemaInterface
    {
        return $this->getSchemaForTopicAndType($topicName, self::BODY_IDX);
    }

    /**
     * @param string $topicName
     * @return KafkaAvroSchemaInterface|null
     * @throws SchemaRegistryException
     */
    public function getKeySchemaForTopic(string $topicName): ?KafkaAvroSchemaInterface
    {
        return $this->getSchemaForTopicAndType($topicName, self::KEY_IDX);
    }

    /**
     * @param string $topicName
     * @return KafkaAvroSchemaInterface|null
     * @throws SchemaRegistryException
     */
    private function getSchemaForTopicAndType(string $topicName, string $type): ?KafkaAvroSchemaInterface
    {
        if (false === isset($this->schemaMapping[$type][$topicName])) {
            return null;
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
     * @param KafkaAvroSchemaInterface $avroSchema
     * @return \AvroSchema
     * @throws SchemaRegistryException
     */
    private function getSchemaDefinition(KafkaAvroSchemaInterface $avroSchema): \AvroSchema
    {
        if (KafkaAvroSchemaInterface::LATEST_VERSION === $avroSchema->getVersion()) {
            return $this->registry->latestVersion($avroSchema->getName());
        }

        return $this->registry->schemaForSubjectAndVersion($avroSchema->getName(), $avroSchema->getVersion());
    }

    /**
     * @return array<string, KafkaAvroSchemaInterface[]>
     */
    public function getTopicSchemaMapping(): array
    {
        return $this->schemaMapping;
    }
}
