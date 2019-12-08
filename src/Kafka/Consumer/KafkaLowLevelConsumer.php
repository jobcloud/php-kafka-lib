<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\Queue as RdKafkaQueue;

final class KafkaLowLevelConsumer extends AbstractKafkaConsumer implements KafkaLowLevelConsumerInterface
{

    /**
     * @var RdKafkaLowLevelConsumer
     */
    protected $consumer;

    /**
     * @var array|RdKafkaConsumerTopic[]
     */
    protected $topics = [];

    /**
     * @var RdKafkaQueue
     */
    protected $queue;

    /**
     * @param RdKafkaLowLevelConsumer $consumer
     * @param KafkaConfiguration      $kafkaConfiguration
     * @param DecoderInterface        $decoder
     */
    public function __construct(
        RdKafkaLowLevelConsumer $consumer,
        KafkaConfiguration $kafkaConfiguration,
        DecoderInterface $decoder
    ) {
        parent::__construct($consumer, $kafkaConfiguration, $decoder);
        $this->queue = $consumer->newQueue();
    }

    /**
     * Subcribes to all defined topics, if no partitions were set, subscribes to all partitions.
     * If partition(s) (and optionally offset(s)) were set, subscribes accordingly
     *
     * @return void
     * @throws KafkaConsumerSubscriptionException
     */
    public function subscribe(): void
    {
        if (true === $this->isSubscribed()) {
            return;
        }

        try {
            $topicSubscriptions = $this->kafkaConfiguration->getTopicSubscriptions();
            foreach ($topicSubscriptions as $topicSubscription) {
                $topicName = $topicSubscription->getTopicName();
                $offset = $topicSubscription->getOffset();

                if (false === isset($this->topics[$topicName])) {
                    $this->topics[$topicName] = $topic = $this->consumer->newTopic($topicName);
                } else {
                    $topic = $this->topics[$topicName];
                }

                $partitions = $topicSubscription->getPartitions();

                if ([] === $partitions) {
                    $partitions = $this->getAllTopicPartitions($topic);
                    $topicSubscription->setPartitions($partitions);
                }

                foreach ($partitions as $partitionId) {
                    $topic->consumeQueueStart($partitionId, $offset, $this->queue);
                }
            }

            $this->subscribed = true;
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Commits the offset to the broker for the given message(s). This is a blocking function
     *
     * @param MessageInterface[]|MessageInterface $messages
     * @return void
     * @throws KafkaConsumerCommitException
     */
    public function commit($messages): void
    {
        $messages = is_array($messages) ? $messages : [$messages];

        foreach ($messages as $i => $message) {
            if (false === $message instanceof KafkaConsumerMessageInterface) {
                throw new KafkaConsumerCommitException(
                    sprintf('Provided message (index: %d) is not an instance of "%s"', $i, KafkaConsumerMessage::class)
                );
            }

            $this->topics[$message->getTopicName()]->offsetStore(
                $message->getPartition(),
                $message->getOffset()
            );
        }
    }

    /**
     * Unsubscribes from the current subscription
     *
     * @return void
     */
    public function unsubscribe(): void
    {
        if (false === $this->isSubscribed()) {
            return;
        }

        $topicSubscriptions = $this->kafkaConfiguration->getTopicSubscriptions();

        /** @var TopicSubscription $topicSubscription */
        foreach ($topicSubscriptions as $topicSubscription) {
            foreach ($topicSubscription->getPartitions() as $partitionId) {
                $this->topics[$topicSubscription->getTopicName()]->consumeStop($partitionId);
            }
        }

        $this->subscribed = false;
    }

    /**
     * @param integer $timeout
     * @return null|RdKafkaMessage
     */
    protected function kafkaConsume(int $timeout): ?RdKafkaMessage
    {
        return $this->queue->consume($timeout);
    }

    /**
     * @param RdKafkaConsumerTopic $topic
     * @return array
     * @throws RdKafkaException
     */
    private function getAllTopicPartitions(RdKafkaConsumerTopic $topic): array
    {

        $partitions = [];
        $topicMetadata = $this->getMetadataForTopic($topic);

        foreach ($topicMetadata->getPartitions() as $partition) {
            $partitions[] = $partition->getId();
        }

        return $partitions;
    }
}
