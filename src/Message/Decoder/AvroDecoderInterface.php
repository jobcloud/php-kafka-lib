<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message\Decoder;

use Jobcloud\Kafka\Message\Registry\AvroSchemaRegistryInterface;

interface AvroDecoderInterface extends DecoderInterface
{
    public const DECODE_ALL = 'all';
    public const DECODE_BODY = 'body';
    public const DECODE_KEY = 'key';

    /**
     * @return AvroSchemaRegistryInterface
     */
    public function getRegistry(): AvroSchemaRegistryInterface;
}
