<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

use RuntimeException;

class KafkaMessageException extends RuntimeException
{
    public const AVRO_BODY_MUST_BE_JSON_MESSAGE = 'The body of an avro message needs to be JSON';
    public const UNABLE_TO_DECODE_PAYLOAD = 'Decoding of the message payload failed';
}
