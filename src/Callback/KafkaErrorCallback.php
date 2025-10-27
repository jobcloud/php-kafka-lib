<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Callback;

use Jobcloud\Kafka\Exception\KafkaBrokerException;

// phpcs:disable
require_once __DIR__ . '/../Exception/KafkaBrokerException.php';  // @codeCoverageIgnore
// phpcs:enable

final class KafkaErrorCallback
{
    /**
     * @throws KafkaBrokerException
     */
    public function __invoke(mixed $kafka, int $errorCode, string $reason): void
    {
        // non fatal errors are retried by librdkafka
        if (RD_KAFKA_RESP_ERR__FATAL !== $errorCode) {
            return;
        }

        throw new KafkaBrokerException(
            $reason,
            $errorCode
        );
    }
}
