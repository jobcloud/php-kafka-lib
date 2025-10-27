<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

use AvroSchema;

final class KafkaAvroSchema implements KafkaAvroSchemaInterface
{
    private string $name;

    public function __construct(
        string $schemaName,
        private int $version = KafkaAvroSchemaInterface::LATEST_VERSION,
        private ?AvroSchema $definition = null,
    ) {
        $this->name = $schemaName;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getVersion(): int
    {
        return $this->version;
    }

    public function setDefinition(AvroSchema $definition): void
    {
        $this->definition = $definition;
    }

    public function getDefinition(): ?AvroSchema
    {
        return $this->definition;
    }
}
