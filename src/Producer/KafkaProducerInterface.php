<?php

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Exception\KafkaProducerTransactionAbortException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionFatalException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionRetryException;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;

interface KafkaProducerInterface
{

    /**
     * @param KafkaProducerMessageInterface $message
     * @return void
     */
    public function produce(KafkaProducerMessageInterface $message): void;

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
     * @param integer $timeout
     * @return integer
     */
    public function flush(int $timeout): int;

    /**
     * Queries the broker for metadata on a certain topic
     *
     * @param string $topicName
     * @return RdKafkaMetadataTopic
     */
    public function getMetadataForTopic(string $topicName): RdKafkaMetadataTopic;

    /**
     * Initialize producer transactions
     *
     * @param int $timeoutMs
     * @return void
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function initTransactions(int $timeoutMs): void;

    /**
     * Start a producer transaction
     *
     * @return void
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function beginTransaction(): void;

    /**
     * Commit the current producer transaction
     *
     * @param int $timeoutMs
     * @return void
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function commitTransaction(int $timeoutMs): void;

    /**
     * Abort the current producer transaction
     *
     * @param int $timeoutMs
     * @return void
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function abortTransaction(int $timeoutMs): void;
}
