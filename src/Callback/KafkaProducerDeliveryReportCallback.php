<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Callback;

use Jobcloud\Kafka\Exception\KafkaProducerException;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\Message as RdKafkaMessage;

// phpcs:disable
require_once __DIR__ . '/../Exception/KafkaProducerException.php';  // @codeCoverageIgnore
// phpcs:enable

final class KafkaProducerDeliveryReportCallback
{
    /**
     * @param RdKafkaProducer $producer
     * @param RdKafkaMessage  $message
     * @return void
     * @throws KafkaProducerException
     */
    public function __invoke(RdKafkaProducer $producer, RdKafkaMessage $message)
    {

        if (RD_KAFKA_RESP_ERR_NO_ERROR === $message->err) {
            return;
        }

        throw new KafkaProducerException(
            $message->errstr(),
            $message->err
        );
    }
}
