<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

final class TopicSubscription implements TopicSubscriptionInterface
{
    /**
     * @param int[] $partitions
     */
    public function __construct(
        private string $topicName,
        private array $partitions = [],
        private int $offset = RD_KAFKA_OFFSET_STORED,
    ) {
    }

    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @param int[] $partitions
     */
    public function setPartitions(array $partitions): void
    {
        $this->partitions = $partitions;
    }

    /**
     * @return int[]
     */
    public function getPartitions(): array
    {
        return $this->partitions;
    }

    public function getOffset(): int
    {
        return $this->offset;
    }
}
