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
     * @param mixed   $kafka
     * @param integer $errorCode
     * @param string  $reason
     * @return void
     * @throws KafkaBrokerException
     */
    public function __invoke($kafka, int $errorCode, string $reason)
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
