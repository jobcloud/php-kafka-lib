<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

use AvroSchema;

final class KafkaAvroSchema implements KafkaAvroSchemaInterface
{
    public function __construct(
        private string $name,
        private int $version = KafkaAvroSchemaInterface::LATEST_VERSION,
        private ?AvroSchema $definition = null,
    ) {
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
