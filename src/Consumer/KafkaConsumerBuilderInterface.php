<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

use Jobcloud\Kafka\Message\Decoder\DecoderInterface;

interface KafkaConsumerBuilderInterface
{
    public const OFFSET_BEGINNING = RD_KAFKA_OFFSET_BEGINNING;
    public const OFFSET_END = RD_KAFKA_OFFSET_END;
    public const OFFSET_STORED = RD_KAFKA_OFFSET_STORED;

    /**
     * Adds a broker from which you want to consume
     */
    public function withAdditionalBroker(string $broker): self;

    /**
     * Add topic name(s) (and additionally partition(s) and offset(s)) to subscribe to
     *
     * @param int[] $partitions
     */
    public function withAdditionalSubscription(
        string $topicName,
        array $partitions = [],
        int $offset = self::OFFSET_STORED
    ): self;

    /**
     * Replaces all topic names previously configured with a topic and additionally partitions and an offset to
     * subscribe to
     *
     * @param int[] $partitions
     */
    public function withSubscription(
        string $topicName,
        array $partitions = [],
        int $offset = self::OFFSET_STORED
    ): self;

    /**
     * Add configuration settings, otherwise the kafka defaults apply
     *
     * @param string[] $config
     */
    public function withAdditionalConfig(array $config): self;

    /**
     * Set the consumer group
     */
    public function withConsumerGroup(string $consumerGroup): self;

    /**
     * Set the consumer type, can be either CONSUMER_TYPE_LOW_LEVEL or CONSUMER_TYPE_HIGH_LEVEL
     */
    public function withConsumerType(string $consumerType): self;

    /**
     * Set a callback to be called on errors.
     * The default callback will throw an exception for every error
     */
    public function withErrorCallback(callable $errorCallback): self;

    /**
     * Set a callback to be called on consumer rebalance
     */
    public function withRebalanceCallback(callable $rebalanceCallback): self;

    /**
     * Only applicable for the high level consumer
     * Callback that is going to be called when you call consume
     */
    public function withConsumeCallback(callable $consumeCallback): self;

    /**
     * Set callback that is being called on offset commits
     */
    public function withOffsetCommitCallback(callable $offsetCommitCallback): self;

    /**
     * Lets you set a custom decoder for the consumed message
     */
    public function withDecoder(DecoderInterface $decoder): self;

    /**
     * Callback for log related events
     */
    public function withLogCallback(callable $logCallback): KafkaConsumerBuilderInterface;

    /**
     * Returns your consumer instance
     */
    public function build(): KafkaConsumerInterface;
}
