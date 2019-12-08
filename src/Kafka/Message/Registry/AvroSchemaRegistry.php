<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Registry;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;

final class AvroSchemaRegistry implements AvroSchemaRegistryInterface
{

    /**
     * @var Registry
     */
    private $registry;

    /**
     * @var array|KafkaAvroSchemaInterface[]
     */
    private $schemaMapping = [];

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
    public function addSchemaMappingForTopic(string $topicName, KafkaAvroSchemaInterface $avroSchema): void
    {
        $this->schemaMapping[$topicName] = $avroSchema;
    }

    /**
     * @param string $topicName
     * @return KafkaAvroSchemaInterface|null
     * @throws SchemaRegistryException
     */
    public function getSchemaForTopic(string $topicName): ?KafkaAvroSchemaInterface
    {
        if (false === isset($this->schemaMapping[$topicName])) {
            return null;
        }

        $avroSchema = $this->schemaMapping[$topicName];

        if (null !== $avroSchema->getDefinition()) {
            return $avroSchema;
        }

        $avroSchema->setDefinition($this->getSchemaDefinition($avroSchema));
        $this->schemaMapping[$topicName] = $avroSchema;

        return $avroSchema;
    }

    /**
     * @param KafkaAvroSchemaInterface $avroSchema
     * @return \AvroSchema
     * @throws SchemaRegistryException
     */
    private function getSchemaDefinition(KafkaAvroSchemaInterface $avroSchema): \AvroSchema
    {
        if (null === $avroSchema->getVersion()) {
            return $this->registry->latestVersion($avroSchema->getName());
        }

        return $this->registry->schemaForSubjectAndVersion($avroSchema->getName(), $avroSchema->getVersion());
    }

    /**
     * @return array|KafkaAvroSchemaInterface[]
     */
    public function getTopicSchemaMapping(): array
    {
        return $this->schemaMapping;
    }
}
