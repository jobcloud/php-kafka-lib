<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

use Jobcloud\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Kafka\Exception\KafkaConsumerTimeoutException;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\TopicPartition as RdKafkaTopicPartition;

/**
 * @todo v2: subscribe(array $topicSubscriptions = [])
 * @method array getTopicSubscriptions()
 */

interface KafkaConsumerInterface
{
    /**
     * Subscribes to all defined topics, if no partitions were set, subscribes to all partitions.
     * If partition(s) (and optionally offset(s)) were set, subscribes accordingly
     *
     * @return void
     */
    public function subscribe(): void;

    /**
     * Unsubscribes from the current subscription / assignment
     *
     * @return void
     */
    public function unsubscribe(): void;

    /**
     * Returns true if the consumer has subscribed to its topics, otherwise false
     * It is mandatory to call `subscribe` before `consume`
     *
     * @return boolean
     */
    public function isSubscribed(): bool;

    /**
     * Consumes a message and returns it
     * In cases of errors / timeouts a KafkaConsumerConsumeException is thrown
     *
     * @param integer $timeoutMs
     * @param boolean $autoDecode
     * @return KafkaConsumerMessageInterface
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerTimeoutException
     */
    public function consume(int $timeoutMs = 10000, bool $autoDecode = true): KafkaConsumerMessageInterface;

    /**
     * Decode consumer message
     *
     * @param KafkaConsumerMessageInterface $message
     * @return KafkaConsumerMessageInterface
     */
    public function decodeMessage(KafkaConsumerMessageInterface $message): KafkaConsumerMessageInterface;

    /**
     * Commits the offset to the broker for the given message(s)
     *
     * @param KafkaConsumerMessageInterface|KafkaConsumerMessageInterface[] $messages
     * @return void
     */
    public function commit($messages): void;

    /**
     * Returns the configuration settings for this consumer instance as array
     *
     * @return string[]
     */
    public function getConfiguration(): array;

    /**
     * Queries the broker for metadata on a certain topic
     *
     * @param string $topicName
     * @param integer $timeoutMs
     * @return RdKafkaMetadataTopic
     */
    public function getMetadataForTopic(string $topicName, int $timeoutMs = 10000): RdKafkaMetadataTopic;

    /**
     * Get the earliest offset for a certain timestamp for topic partitions
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @param integer                       $timeoutMs
     * @return array|RdKafkaTopicPartition[]
     */
    public function offsetsForTimes(array $topicPartitions, int $timeoutMs): array;

    /**
     * Queries the broker for the first offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeoutMs
     * @return integer
     */
    public function getFirstOffsetForTopicPartition(string $topic, int $partition, int $timeoutMs): int;

    /**
     * Queries the broker for the last offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeoutMs
     * @return integer
     */
    public function getLastOffsetForTopicPartition(string $topic, int $partition, int $timeoutMs): int;

    /**
     * @todo v2
     *
     * @return array<int, TopicSubscription>
     */
    //public function getTopicSubscriptions(): array;
}
