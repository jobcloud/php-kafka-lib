<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Encoder;

use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;

interface AvroEncoderInterface extends EncoderInterface
{
    public const ENCODE_ALL = 'all';
    public const ENCODE_BODY = 'body';
    public const ENCODE_KEY = 'key';

    /**
     * @return AvroSchemaRegistryInterface
     */
    public function getRegistry(): AvroSchemaRegistryInterface;
}
