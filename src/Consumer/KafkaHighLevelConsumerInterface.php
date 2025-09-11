<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use RdKafka\TopicPartition as RdKafkaTopicPartition;

interface KafkaHighLevelConsumerInterface extends KafkaConsumerInterface
{
    /**
     * Assigns a consumer to the given TopicPartition(s)
     *
     * @param string[]|RdKafkaTopicPartition[] $topicPartitions
     */
    public function assign(array $topicPartitions): void;

    /**
     * Asynchronous version of commit (non-blocking)
     *
     * @param KafkaConsumerMessageInterface|KafkaConsumerMessageInterface[] $messages
     */
    public function commitAsync(array|KafkaConsumerMessageInterface $messages): void;

    /**
     * Gets the current assignment for the consumer
     *
     * @return RdKafkaTopicPartition[]
     */
    public function getAssignment(): array;

    /**
     * Gets the commited offset for a TopicPartition for the configured consumer group
     *
     * @param RdKafkaTopicPartition[] $topicPartitions
     * @return RdKafkaTopicPartition[]
     */
    public function getCommittedOffsets(array $topicPartitions, int $timeoutMs): array;

    /**
     * Get current offset positions of the consumer
     *
     * @param RdKafkaTopicPartition[] $topicPartitions
     * @return RdKafkaTopicPartition[]
     */
    public function getOffsetPositions(array $topicPartitions): array;

    /**
     * Close the consumer connection
     */
    public function close(): void;
}
