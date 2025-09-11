<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Callback;

use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Kafka\Exception\KafkaRebalanceException;
use RdKafka\Exception as RdKafkaException;
use RdKafka\TopicPartition as RdKafkaTopicPartition;

// phpcs:disable
require_once __DIR__ . '/../Exception/KafkaRebalanceException.php'; // @codeCoverageIgnore
// phpcs:enable

final class KafkaConsumerRebalanceCallback
{
    /**
     * @param RdKafkaTopicPartition[]|null $partitions
     * @throws KafkaRebalanceException
     */
    public function __invoke(RdKafkaConsumer $consumer, int $errorCode, ?array $partitions = null): void
    {
        try {
            if ($errorCode === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                $consumer->assign($partitions);
            } else {
                $consumer->assign();
            }
        } catch (RdKafkaException $e) {
            throw new KafkaRebalanceException($e->getMessage(), $e->getCode(), $e);
        }
    }
}
