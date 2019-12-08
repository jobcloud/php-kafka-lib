<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

interface KafkaAvroSchemaInterface
{
    /**
     * @return string
     */
    public function getName(): string;

    /**
     * @return integer|null
     */
    public function getVersion(): ?int;

    /**
     * @param \AvroSchema $definition
     * @return void
     */
    public function setDefinition(\AvroSchema $definition): void;

    /**
     * @return \AvroSchema|null
     */
    public function getDefinition(): ?\AvroSchema;
}
