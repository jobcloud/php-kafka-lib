<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

final class TopicSubscription implements TopicSubscriptionInterface
{
    /**
     * @var string
     */
    private $topicName;

    /**
     * @var int[]
     */
    private $partitions = [];

    /**
     * @var int|null
     */
    private $offset;

    /**
     * @param string  $topicName
     * @param int[]   $partitions
     * @param integer $offset
     */
    public function __construct(
        string $topicName,
        array $partitions = [],
        int $offset = RD_KAFKA_OFFSET_STORED
    ) {
        $this->topicName = $topicName;
        $this->partitions = $partitions;
        $this->offset = $offset;
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @param int[] $partitions
     * @return void
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

    /**
     * @return integer
     */
    public function getOffset(): int
    {
        return $this->offset ?? RD_KAFKA_OFFSET_STORED;
    }
}
