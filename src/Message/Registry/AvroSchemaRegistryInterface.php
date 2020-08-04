<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Registry;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Kafka\Message\KafkaAvroSchemaInterface;

interface AvroSchemaRegistryInterface
{

    /**
     * @param string                   $topicName
     * @param KafkaAvroSchemaInterface $avroSchema
     * @return void
     */
    public function addBodySchemaMappingForTopic(string $topicName, KafkaAvroSchemaInterface $avroSchema): void;

    /**
     * @param string                   $topicName
     * @param KafkaAvroSchemaInterface $avroSchema
     * @return void
     */
    public function addKeySchemaMappingForTopic(string $topicName, KafkaAvroSchemaInterface $avroSchema): void;

    /**
     * @return array<string, KafkaAvroSchemaInterface[]>
     */
    public function getTopicSchemaMapping(): array;

    /**
     * @param string $topicName
     * @return KafkaAvroSchemaInterface|null
     * @throws SchemaRegistryException
     */
    public function getBodySchemaForTopic(string $topicName): ?KafkaAvroSchemaInterface;

    /**
     * @param string $topicName
     * @return KafkaAvroSchemaInterface|null
     * @throws SchemaRegistryException
     */
    public function getKeySchemaForTopic(string $topicName): ?KafkaAvroSchemaInterface;
}
