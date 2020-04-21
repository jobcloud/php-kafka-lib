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
     * @return void
     */
    public function assign(array $topicPartitions): void;

    /**
     * Asynchronous version of commit (non blocking)
     *
     * @param KafkaConsumerMessageInterface|KafkaConsumerMessageInterface[] $messages
     * @return void
     */
    public function commitAsync($messages): void;

    /**
     * Gets the current assignment for the consumer
     *
     * @return array|RdKafkaTopicPartition[]
     */
    public function getAssignment(): array;

    /**
     * Gets the commited offset for a TopicPartition for the configured consumer group
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @param integer                       $timeoutMs
     * @return array|RdKafkaTopicPartition[]
     */
    public function getCommittedOffsets(array $topicPartitions, int $timeoutMs): array;

    /**
     * Get current offset positions of the consumer
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @return array|RdKafkaTopicPartition[]
     */
    public function getOffsetPositions(array $topicPartitions): array;

    /**
     * Close the consumer connection
     *
     * @return void;
     */
    public function close(): void;
}
