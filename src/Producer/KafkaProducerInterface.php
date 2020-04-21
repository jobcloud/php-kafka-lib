<?php

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;

interface KafkaProducerInterface
{

    /**
     * @param KafkaProducerMessageInterface $message
     * @param integer $pollTimeoutMs
     * @return void
     */
    public function produce(KafkaProducerMessageInterface $message, int $pollTimeoutMs = 0): void;

    /**
     * Purge producer messages that are in flight
     *
     * @param integer $purgeFlags
     * @return integer
     */
    public function purge(int $purgeFlags): int;

    /**
     * Wait until all outstanding produce requests are completed
     *
     * @param integer $timeoutMs
     * @return integer
     */
    public function flush(int $timeoutMs): int;

    /**
     * Queries the broker for metadata on a certain topic
     *
     * @param string $topicName
     * @param integer $timeoutMs
     * @return RdKafkaMetadataTopic
     */
    public function getMetadataForTopic(string $topicName, int $timeoutMs = 10000): RdKafkaMetadataTopic;
}
