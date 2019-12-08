<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerAssignmentException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerRequestException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Messaging\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message as RdKafkaMessage;
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
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function subscribe(): void
    {
        $subscriptions = $this->getTopicSubscriptions();
        $assignments = $this->getTopicAssignments();

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
     * @param array $topicPartitions
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
     * @return array
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
     * @param integer                       $timeout
     * @return array|RdKafkaTopicPartition[]
     * @throws KafkaConsumerRequestException
     */
    public function getCommittedOffsets(array $topicPartitions, int $timeout): array
    {
        try {
            return $this->consumer->getCommittedOffsets($topicPartitions, $timeout);
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerRequestException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * Get current offset positions of the consumer
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @return array
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
     * @param integer $timeout
     * @return RdKafkaMessage|null
     * @throws RdKafkaException
     */
    protected function kafkaConsume(int $timeout): ?RdKafkaMessage
    {
        return $this->consumer->consume($timeout);
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
     * @param array $messages
     * @return array
     */
    private function getOffsetsToCommitForMessages(array $messages): array
    {
        $offsetsToCommit = [];

        foreach ($messages as $message) {
            $topicPartition = sprintf('%s-%s', $message->getTopicName(), $message->getPartition());

            if (true === isset($offsetsToCommit[$topicPartition])) {
                if ($message->getOffset() + 1 > $offsetsToCommit[$topicPartition]) {
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
     * @return array|string[]
     */
    private function getTopicSubscriptions(): array
    {
        $subscriptions = [];

        foreach ($this->kafkaConfiguration->getTopicSubscriptions() as $topicSubscription) {
            if ([] !== $topicSubscription->getPartitions()) {
                continue;
            }
            $subscriptions[] = $topicSubscription->getTopicName();
        }

        return $subscriptions;
    }

    /**
     * @return array|RdKafkaTopicPartition[]
     */
    private function getTopicAssignments(): array
    {
        $assignments = [];

        foreach ($this->kafkaConfiguration->getTopicSubscriptions() as $topicSubscription) {
            if ([] === $topicSubscription->getPartitions()) {
                continue;
            }

            $offset = $topicSubscription->getOffset();

            foreach ($topicSubscription->getPartitions() as $partitionId) {
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
