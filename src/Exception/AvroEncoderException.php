<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

class AvroEncoderException extends \RuntimeException
{
    public const MESSAGE_BODY_MUST_BE_JSON_MESSAGE = 'The body of an avro message must be JSON';
    public const NO_BODY_SCHEMA_FOR_TOPIC_MESSAGE = 'There is no body avro schema defined for the topic %s';
    public const NO_KEY_SCHEMA_FOR_TOPIC_MESSAGE = 'There is no key avro schema defined for the topic %s';
    public const UNABLE_TO_LOAD_DEFINITION_MESSAGE = 'Was unable to load definition for schema %s';
}
