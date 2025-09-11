<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

interface TopicSubscriptionInterface
{
    public function getTopicName(): string;

    /**
     * @return int[]
     */
    public function getPartitions(): array;

    /**
     * @param int[] $partitions
     */
    public function setPartitions(array $partitions): void;

    public function getOffset(): int;
}
