<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Exception;

use RuntimeException;

class KafkaSllCertsCreationException extends RuntimeException
{
    public const UNABLE_TO_CREATE_KAFKA_CERT = 'Unable to create Kafka cert';
}
