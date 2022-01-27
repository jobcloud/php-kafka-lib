<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

use Jobcloud\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Kafka\Exception\KafkaConsumerTimeoutException;
use Jobcloud\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Kafka\Message\KafkaConsumerMessage;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
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
     * @return string[]
     */
    public function getConfiguration(): array
    {
        return $this->kafkaConfiguration->dump();
    }

    /**
     * Consumes a message and returns it
     * In cases of errors / timeouts an exception is thrown
     *
     * @param integer $timeoutMs
     * @param boolean $autoDecode
     * @return KafkaConsumerMessageInterface
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerTimeoutException
     */
    public function consume(int $timeoutMs = 10000, bool $autoDecode = true): KafkaConsumerMessageInterface
    {
        if (false === $this->isSubscribed()) {
            throw new KafkaConsumerConsumeException(KafkaConsumerConsumeException::NOT_SUBSCRIBED_EXCEPTION_MESSAGE);
        }

        if (null === $rdKafkaMessage = $this->kafkaConsume($timeoutMs)) {
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

        if (true === $autoDecode) {
            return $this->decoder->decode($message);
        }

        return $message;
    }

    /**
     * Decode consumer message
     *
     * @param KafkaConsumerMessageInterface $message
     * @return KafkaConsumerMessageInterface
     */
    public function decodeMessage(KafkaConsumerMessageInterface $message): KafkaConsumerMessageInterface
    {
        return $this->decoder->decode($message);
    }

    /**
     * Queries the broker for metadata on a certain topic
     *
     * @param string $topicName
     * @param integer $timeoutMs
     * @return RdKafkaMetadataTopic
     * @throws RdKafkaException
     */
    public function getMetadataForTopic(string $topicName, int $timeoutMs = 10000): RdKafkaMetadataTopic
    {
        $topic = $this->consumer->newTopic($topicName);
        return $this->consumer
            ->getMetadata(
                false,
                $topic,
                $timeoutMs
            )
            ->getTopics()
            ->current();
    }

    /**
     * Get the earliest offset for a certain timestamp for topic partitions
     *
     * @param array|RdKafkaTopicPartition[] $topicPartitions
     * @param integer                       $timeoutMs
     * @return array|RdKafkaTopicPartition[]
     */
    public function offsetsForTimes(array $topicPartitions, int $timeoutMs): array
    {
        return $this->consumer->offsetsForTimes($topicPartitions, $timeoutMs);
    }

    /**
     * Queries the broker for the first offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeoutMs
     * @return integer
     */
    public function getFirstOffsetForTopicPartition(string $topic, int $partition, int $timeoutMs): int
    {
        $lowOffset = 0;
        $highOffset = 0;

        $this->consumer->queryWatermarkOffsets($topic, $partition, $lowOffset, $highOffset, $timeoutMs);

        return $lowOffset;
    }

    /**
     * Queries the broker for the last offset of a given topic and partition
     *
     * @param string  $topic
     * @param integer $partition
     * @param integer $timeoutMs
     * @return integer
     */
    public function getLastOffsetForTopicPartition(string $topic, int $partition, int $timeoutMs): int
    {
        $lowOffset = 0;
        $highOffset = 0;

        $this->consumer->queryWatermarkOffsets($topic, $partition, $lowOffset, $highOffset, $timeoutMs);

        return $highOffset;
    }

    /**
     * @param string $topic
     * @return int[]
     * @throws RdKafkaException
     */
    protected function getAllTopicPartitions(string $topic): array
    {

        $partitions = [];
        $topicMetadata = $this->getMetadataForTopic($topic);

        foreach ($topicMetadata->getPartitions() as $partition) {
            $partitions[] = $partition->getId();
        }

        return $partitions;
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
            $message->key,
            $message->payload,
            (array) $message->headers
        );
    }

    /**
     * @return array<int, TopicSubscription>
     */
    public function getTopicSubscriptions(): array
    {
        return $this->kafkaConfiguration->getTopicSubscriptions();
    }

    /**
     * @param integer $timeoutMs
     * @return null|RdKafkaMessage
     */
    abstract protected function kafkaConsume(int $timeoutMs): ?RdKafkaMessage;
}
