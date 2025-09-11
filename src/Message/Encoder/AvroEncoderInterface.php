<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Encoder;

use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;

interface AvroEncoderInterface extends EncoderInterface
{
    public function getRegistry(): AvroSchemaRegistryInterface;
}
