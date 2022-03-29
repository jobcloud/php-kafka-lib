<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

use Jobcloud\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Kafka\Exception\KafkaConsumerAssignmentException;
use Jobcloud\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Kafka\Exception\KafkaConsumerRequestException;
use Jobcloud\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\TopicPartition;
use RdKafka\TopicPartition as RdKafkaTopicPartition;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;

final class KafkaHighLevelConsumer extends AbstractKafkaConsumer implements KafkaHighLevelConsumerInterface
{
    /**
     * @var RdKafkaHighLevelConsumer
     */
    protected $consumer;

    /**
     * @param RdKafkaHighLevelConsumer $consumer
     * @param KafkaConfiguration       $kafkaConfiguration
     * @param DecoderInterface         $decoder
     */
    public function __construct(
        RdKafkaHighLevelConsumer $consumer,
        KafkaConfiguration $kafkaConfiguration,
        DecoderInterface $decoder
    ) {
        parent::__construct($consumer, $kafkaConfiguration, $decoder);
    }

    /**
     * Subscribes to all defined topics, if no partitions were set, subscribes to all partitions.
     * If partition(s) (and optionally offset(s)) were set, subscribes accordingly
     *
     * @param array<TopicSubscription> $topicSubscriptions
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function subscribe(array $topicSubscriptions = []): void
    {
        $subscriptions = $this->getTopicSubscriptionNames($topicSubscriptions);
        $assignments = $this->getTopicAssignments($topicSubscriptions);

        if ([] !== $subscriptions && [] !== $assignments) {
            throw new KafkaConsumerSubscriptionException(
                KafkaConsumerSubscriptionException::MIXED_SUBSCRIPTION_EXCEPTION_MESSAGE
            );
        }

        try {
            if ([] !== $subscriptions) {
                $this->consumer->subscribe($subscriptions);
            } else {
                $this->consumer->assign($assignments);
            }
            $this->subscribed = true;
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Unsubscribes from the current subscription / assignment
     *
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function unsubscribe(): void
    {
        try {
            $this->consumer->unsubscribe();
            $this->subscribed = false;
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Commits the offset to the broker for the given message(s)
     * This is a blocking function, checkout out commitAsync if you want to commit in a non blocking manner
     *
     * @param KafkaConsumerMessageInterface|KafkaConsumerMessageInterface[] $messages
     * @return void
     * @throws KafkaConsumerCommitException
     */
    public function commit($messages): void
    {
        $this->commitMessages($messages);
    }

    /**
     * Assigns a consumer to the given TopicPartition(s)
     *
     * @param RdKafkaTopicPartition[] $topicPartitions
     * @throws KafkaConsumerAssignmentException
     * @return void
     */
    public function assign(array $topicPartitions): void
    {
        try {
            $this->consumer->assign($topicPartitions);
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerAssignmentException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * Asynchronous version of commit (non blocking)
     *
     * @param KafkaConsumerMessageInterface|KafkaConsumerMessageInterface[] $messages
     * @return void
     * @throws KafkaConsumerCommitException
     */
    public function commitAsync($messages): void
    {
        $this->commitMessages($messages, true);
    }

    /**
     * Gets the current assignment for the consumer
     *
     * @return array|RdKafkaTopicPartition[]
     * @throws KafkaConsumerAssignmentException
     */
    public function getAssignment(): array
    {
        try {
            return $this->consumer->getAssignment();
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerAssignmentException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * Gets the commited offset for a TopicPartition for the configured consumer group
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @param integer                       $timeoutMs
     * @return array|RdKafkaTopicPartition[]
     * @throws KafkaConsumerRequestException
     */
    public function getCommittedOffsets(array $topicPartitions, int $timeoutMs): array
    {
        try {
            return $this->consumer->getCommittedOffsets($topicPartitions, $timeoutMs);
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerRequestException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * Get current offset positions of the consumer
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @return array|RdKafkaTopicPartition[]
     */
    public function getOffsetPositions(array $topicPartitions): array
    {
        return $this->consumer->getOffsetPositions($topicPartitions);
    }

    /**
     * Close the consumer connection
     *
     * @return void;
     */
    public function close(): void
    {
        $this->consumer->close();
    }

    /**
     * @param integer $timeoutMs
     * @return RdKafkaMessage|null
     * @throws RdKafkaException
     */
    protected function kafkaConsume(int $timeoutMs): ?RdKafkaMessage
    {
        return $this->consumer->consume($timeoutMs);
    }

    /**
     * @param KafkaConsumerMessageInterface|KafkaConsumerMessageInterface[] $messages
     * @param boolean                                                       $asAsync
     * @return void
     * @throws KafkaConsumerCommitException
     */
    private function commitMessages($messages, bool $asAsync = false): void
    {
        $messages = is_array($messages) ? $messages : [$messages];

        $offsetsToCommit = $this->getOffsetsToCommitForMessages($messages);

        try {
            if (true === $asAsync) {
                $this->consumer->commitAsync($offsetsToCommit);
            } else {
                $this->consumer->commit($offsetsToCommit);
            }
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerCommitException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * @param array|KafkaConsumerMessageInterface[] $messages
     * @return array|RdKafkaTopicPartition[]
     */
    private function getOffsetsToCommitForMessages(array $messages): array
    {
        $offsetsToCommit = [];

        foreach ($messages as $message) {
            $topicPartition = sprintf('%s-%s', $message->getTopicName(), $message->getPartition());

            if (true === isset($offsetsToCommit[$topicPartition])) {
                if ($message->getOffset() + 1 > $offsetsToCommit[$topicPartition]->getOffset()) {
                    $offsetsToCommit[$topicPartition]->setOffset($message->getOffset() + 1);
                }
                continue;
            }

            $offsetsToCommit[$topicPartition] = new RdKafkaTopicPartition(
                $message->getTopicName(),
                $message->getPartition(),
                $message->getOffset() + 1
            );
        }

        return $offsetsToCommit;
    }

    /**
     * @param array<TopicSubscription> $topicSubscriptions
     * @return array|string[]
     */
    private function getTopicSubscriptionNames(array $topicSubscriptions = []): array
    {
        $subscriptions = [];

        if ([] === $topicSubscriptions) {
            $topicSubscriptions = $this->kafkaConfiguration->getTopicSubscriptions();
        }

        foreach ($topicSubscriptions as $topicSubscription) {
            if (
                [] !== $topicSubscription->getPartitions()
                || KafkaConsumerBuilderInterface::OFFSET_STORED !== $topicSubscription->getOffset()
            ) {
                continue;
            }
            $subscriptions[] = $topicSubscription->getTopicName();
        }

        return $subscriptions;
    }

    /**
     * @param array<TopicSubscription> $topicSubscriptions
     * @return array|RdKafkaTopicPartition[]
     */
    private function getTopicAssignments(array $topicSubscriptions = []): array
    {
        $assignments = [];

        if ([] === $topicSubscriptions) {
            $topicSubscriptions = $this->kafkaConfiguration->getTopicSubscriptions();
        }

        foreach ($topicSubscriptions as $topicSubscription) {
            if (
                [] === $topicSubscription->getPartitions()
                && KafkaConsumerBuilderInterface::OFFSET_STORED === $topicSubscription->getOffset()
            ) {
                continue;
            }

            $offset = $topicSubscription->getOffset();
            $partitions = $topicSubscription->getPartitions();

            if ([] === $partitions) {
                $partitions = $this->getAllTopicPartitions($topicSubscription->getTopicName());
            }

            foreach ($partitions as $partitionId) {
                $assignments[] = new RdKafkaTopicPartition(
                    $topicSubscription->getTopicName(),
                    $partitionId,
                    $offset
                );
            }
        }

        return $assignments;
    }
}
