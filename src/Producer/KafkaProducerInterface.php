<?php

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Exception\KafkaProducerTransactionAbortException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionFatalException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionRetryException;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;

interface KafkaProducerInterface
{
    public function produce(
        KafkaProducerMessageInterface $message,
        bool $autoPoll = true,
        int $pollTimeoutMs = 0
    ): void;

    /**
     * Produces a message to the topic and partition defined in the message
     * If a schema name was given, the message body will be avro serialized.
     * Wait for the message to event to arrive before continuing (blocking)
     */
    public function syncProduce(KafkaProducerMessageInterface $message): void;

    /**
     * Poll for producer event, pass 0 for non-blocking, pass -1 to block until an event arrives
     */
    public function poll(int $timeoutMs = 0): void;

    /**
     * Poll for producer events until the number of $queueSize events remain
     */
    public function pollUntilQueueSizeReached(int $timeoutMs = 0, int $queueSize = 0): void;

    /**
     * Purge producer messages that are in flight
     */
    public function purge(int $purgeFlags): int;

    /**
     * Wait until all outstanding produce requests are completed
     */
    public function flush(int $timeoutMs): int;

    /**
     * Queries the broker for metadata on a certain topic
     */
    public function getMetadataForTopic(string $topicName, int $timeoutMs = 10000): RdKafkaMetadataTopic;

    /**
     * Start a producer transaction
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function beginTransaction(int $timeoutMs): void;

    /**
     * Commit the current producer transaction
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function commitTransaction(int $timeoutMs): void;

    /**
     * Abort the current producer transaction
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function abortTransaction(int $timeoutMs): void;
}
