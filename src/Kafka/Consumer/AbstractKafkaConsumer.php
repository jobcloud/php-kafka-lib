<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerTimeoutException;
use Jobcloud\Messaging\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\TopicPartition as RdKafkaTopicPartition;

abstract class AbstractKafkaConsumer implements KafkaConsumerInterface
{

    /**
     * @var KafkaConfiguration
     */
    protected $kafkaConfiguration;

    /**
     * @var boolean
     */
    protected $subscribed = false;

    /**
     * @var RdKafkaLowLevelConsumer|RdKafkaHighLevelConsumer
     */
    protected $consumer;

    /**
     * @var DecoderInterface
     */
    protected $decoder;

    /**
     * @param mixed              $consumer
     * @param KafkaConfiguration $kafkaConfiguration
     * @param DecoderInterface   $decoder
     */
    public function __construct(
        $consumer,
        KafkaConfiguration $kafkaConfiguration,
        DecoderInterface $decoder
    ) {
        $this->consumer = $consumer;
        $this->kafkaConfiguration = $kafkaConfiguration;
        $this->decoder = $decoder;
    }

    /**
     * Returns true if the consumer has subscribed to its topics, otherwise false
     * It is mandatory to call `subscribe` before `consume`
     *
     * @return boolean
     */
    public function isSubscribed(): bool
    {
        return $this->subscribed;
    }

    /**
     * Returns the configuration settings for this consumer instance as array
     *
     * @return array
     */
    public function getConfiguration(): array
    {
        return $this->kafkaConfiguration->dump();
    }

    /**
     * Consumes a message and returns it
     * In cases of errors / timeouts an exception is thrown
     *
     * @return MessageInterface
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerTimeoutException
     */
    public function consume(): MessageInterface
    {
        if (false === $this->isSubscribed()) {
            throw new KafkaConsumerConsumeException(KafkaConsumerConsumeException::NOT_SUBSCRIBED_EXCEPTION_MESSAGE);
        }

        if (null === $rdKafkaMessage = $this->kafkaConsume($this->kafkaConfiguration->getTimeout())) {
            throw new KafkaConsumerEndOfPartitionException(
                rd_kafka_err2str(RD_KAFKA_RESP_ERR__PARTITION_EOF),
                RD_KAFKA_RESP_ERR__PARTITION_EOF
            );
        }

        if (RD_KAFKA_RESP_ERR__PARTITION_EOF === $rdKafkaMessage->err) {
            throw new KafkaConsumerEndOfPartitionException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        } elseif (RD_KAFKA_RESP_ERR__TIMED_OUT === $rdKafkaMessage->err) {
            throw new KafkaConsumerTimeoutException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        }

        $message = $this->getConsumerMessage($rdKafkaMessage);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err, $message);
        }

        return $this->decoder->decode($message);
    }

    /**
     * Queries the broker for metadata on a certain topic
     *
     * @param RdKafkaConsumerTopic $topic
     * @return RdKafkaMetadataTopic
     * @throws RdKafkaException
     */
    public function getMetadataForTopic(RdKafkaConsumerTopic $topic): RdKafkaMetadataTopic
    {
        return $this->consumer
            ->getMetadata(
                false,
                $topic,
                $this->kafkaConfiguration->getTimeout()
            )
            ->getTopics()
            ->current();
    }

    /**
     * Get the earliest offset for a certain timestamp for topic partitions
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @param integer                       $timeout
     * @return array
     */
    public function offsetsForTimes(array $topicPartitions, int $timeout): array
    {
        return $this->consumer->offsetsForTimes($topicPartitions, $timeout);
    }

    /**
     * Queries the broker for the first offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeout
     * @return integer
     */
    public function getFirstOffsetForTopicPartition(string $topic, int $partition, int $timeout): int
    {
        $lowOffset = 0;
        $highOffset = 0;

        $this->consumer->queryWatermarkOffsets($topic, $partition, $lowOffset, $highOffset, $timeout);

        return $lowOffset;
    }

    /**
     * Queries the broker for the last offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeout
     * @return integer
     */
    public function getLastOffsetForTopicPartition(string $topic, int $partition, int $timeout): int
    {
        $lowOffset = 0;
        $highOffset = 0;

        $this->consumer->queryWatermarkOffsets($topic, $partition, $lowOffset, $highOffset, $timeout);

        return $highOffset;
    }

    /**
     * @param RdKafkaMessage $message
     * @return KafkaConsumerMessageInterface
     */
    protected function getConsumerMessage(RdKafkaMessage $message): KafkaConsumerMessageInterface
    {
        return new KafkaConsumerMessage(
            (string) $message->topic_name,
            (int) $message->partition,
            (int) $message->offset,
            (int) $message->timestamp,
            (string) $message->key,
            (string) $message->payload,
            (array) $message->headers
        );
    }

    /**
     * @param integer $timeout
     * @return null|RdKafkaMessage
     */
    abstract protected function kafkaConsume(int $timeout): ?RdKafkaMessage;
}
