<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Decoder;

use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistryInterface;

interface AvroDecoderInterface extends DecoderInterface
{
    /**
     * @return AvroSchemaRegistryInterface
     */
    public function getRegistry(): AvroSchemaRegistryInterface;
}
