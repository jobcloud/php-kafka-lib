<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;
use RdKafka\TopicPartition as RdKafkaTopicPartition;

interface KafkaConsumerInterface extends ConsumerInterface
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
     * @return KafkaConsumerMessageInterface
     */
    public function consume(): MessageInterface;

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
     * @return array
     */
    public function getConfiguration(): array;

    /**
     * Queries the broker for metadata on a certain topic
     *
     * @param RdKafkaConsumerTopic $topic
     * @return RdKafkaMetadataTopic
     */
    public function getMetadataForTopic(RdKafkaConsumerTopic $topic): RdKafkaMetadataTopic;

    /**
     * Get the earliest offset for a certain timestamp for topic partitions
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @param integer                       $timeout
     * @return array
     */
    public function offsetsForTimes(array $topicPartitions, int $timeout): array;

    /**
     * Queries the broker for the first offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeout
     * @return integer
     */
    public function getFirstOffsetForTopicPartition(string $topic, int $partition, int $timeout): int;

    /**
     * Queries the broker for the last offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeout
     * @return integer
     */
    public function getLastOffsetForTopicPartition(string $topic, int $partition, int $timeout): int;
}
