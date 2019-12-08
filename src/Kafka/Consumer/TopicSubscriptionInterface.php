<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface TopicSubscriptionInterface
{

    /**
     * @return string
     */
    public function getTopicName(): string;

    /**
     * @return array
     */
    public function getPartitions(): array;

    /**
     * @param array $partitions
     * @return void
     */
    public function setPartitions(array $partitions): void;

    /**
     * @return integer
     */
    public function getOffset(): int;
}
