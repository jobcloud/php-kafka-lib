<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Kafka\Message\Encoder\EncoderInterface;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\Exception as RdKafkaException;

final class KafkaProducer implements KafkaProducerInterface
{

    /**
     * @var RdKafkaProducer
     */
    protected $producer;

    /**
     * @var KafkaConfiguration
     */
    protected $kafkaConfiguration;

    /**
     * @var array
     */
    protected $producerTopics = [];

    /**
     * @var EncoderInterface
     */
    protected $encoder;

    /**
     * KafkaProducer constructor.
     * @param RdKafkaProducer    $producer
     * @param KafkaConfiguration $kafkaConfiguration
     * @param EncoderInterface   $encoder
     */
    public function __construct(
        RdKafkaProducer $producer,
        KafkaConfiguration $kafkaConfiguration,
        EncoderInterface $encoder
    ) {
        $this->producer = $producer;
        $this->kafkaConfiguration = $kafkaConfiguration;
        $this->encoder = $encoder;
    }

    /**
     * Produces a message to the topic and partition defined in the message
     * If a schema name was given, the message body will be avro serialized.
     *
     * @param KafkaProducerMessageInterface $message
     * @return void
     */
    public function produce(KafkaProducerMessageInterface $message): void
    {
        $message = $this->encoder->encode($message);

        /** @var KafkaProducerMessageInterface $message */
        $topicProducer = $this->getProducerTopicForTopic($message->getTopicName());

        $topicProducer->producev(
            $message->getPartition(),
            RD_KAFKA_MSG_F_BLOCK,
            $message->getBody(),
            $message->getKey(),
            $message->getHeaders()
        );

        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll($this->kafkaConfiguration->getTimeout());
        }
    }

    /**
     * Purge producer messages that are in flight
     *
     * @param integer $purgeFlags
     * @return integer
     */
    public function purge(int $purgeFlags): int
    {
        return $this->producer->purge($purgeFlags);
    }

    /**
     * Wait until all outstanding produce requests are completed
     *
     * @param integer $timeout
     * @return integer
     */
    public function flush(int $timeout): int
    {
        return $this->producer->flush($timeout);
    }

    /**
     * Queries the broker for metadata on a certain topic
     *
     * @param string $topicName
     * @return RdKafkaMetadataTopic
     * @throws RdKafkaException
     */
    public function getMetadataForTopic(string $topicName): RdKafkaMetadataTopic
    {
        $topic = $this->producer->newTopic($topicName);
        return $this->producer
            ->getMetadata(
                false,
                $topic,
                $this->kafkaConfiguration->getTimeout()
            )
            ->getTopics()
            ->current();
    }

    /**
     * @param string $topic
     * @return RdKafkaProducerTopic
     */
    private function getProducerTopicForTopic(string $topic): RdKafkaProducerTopic
    {
        if (!isset($this->producerTopics[$topic])) {
            $this->producerTopics[$topic] = $this->producer->newTopic($topic);
        }

        return $this->producerTopics[$topic];
    }

}
