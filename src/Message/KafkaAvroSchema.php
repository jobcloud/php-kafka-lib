<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

final class KafkaAvroSchema implements KafkaAvroSchemaInterface
{
    /**
     * @var string
     */
    private $name;

    /**
     * @var integer
     */
    private $version;

    /**
     * @var \AvroSchema|null
     */
    private $definition;

    /**
     * KafkaAvroSchema constructor.
     * @param string           $schemaName
     * @param integer     $version
     * @param \AvroSchema|null $definition
     */
    public function __construct(
        string $schemaName,
        int $version = KafkaAvroSchemaInterface::LATEST_VERSION,
        ?\AvroSchema $definition = null
    ) {
        $this->name = $schemaName;
        $this->version = $version;
        $this->definition = $definition;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return integer
     */
    public function getVersion(): int
    {
        return $this->version;
    }

    /**
     * @param \AvroSchema $definition
     * @return void
     */
    public function setDefinition(\AvroSchema $definition): void
    {
        $this->definition = $definition;
    }

    /**
     * @return \AvroSchema|null
     */
    public function getDefinition(): ?\AvroSchema
    {
        return $this->definition;
    }
}
