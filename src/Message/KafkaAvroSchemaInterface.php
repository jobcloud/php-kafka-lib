<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

use AvroSchema;

interface KafkaAvroSchemaInterface
{
    public const LATEST_VERSION = -1;

    public function getName(): string;

    public function getVersion(): int;

    public function setDefinition(AvroSchema $definition): void;

    public function getDefinition(): ?AvroSchema;
}
