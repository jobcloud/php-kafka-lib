<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Exception\KafkaProducerTransactionAbortException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionFatalException;
use Jobcloud\Kafka\Exception\KafkaProducerTransactionRetryException;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Kafka\Message\Encoder\EncoderInterface;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\KafkaErrorException as RdKafkaErrorException;

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
     * @var array|RdKafkaProducerTopic[]
     */
    protected $producerTopics = [];

    /**
     * @var EncoderInterface
     */
    protected $encoder;

    /**
     * @var bool
     */
    private $transactionInitialized = false;

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
     * @param boolean $autoPoll
     * @param integer $pollTimeoutMs
     * @return void
     */
    public function produce(KafkaProducerMessageInterface $message, bool $autoPoll = true, int $pollTimeoutMs = 0): void
    {
        $message = $this->encoder->encode($message);

        $topicProducer = $this->getProducerTopicForTopic($message->getTopicName());

        $topicProducer->producev(
            $message->getPartition(),
            RD_KAFKA_MSG_F_BLOCK,
            $message->getBody(),
            $message->getKey(),
            $message->getHeaders()
        );

        if (true === $autoPoll) {
            $this->producer->poll($pollTimeoutMs);
        }
    }

    /**
     * Produces a message to the topic and partition defined in the message
     * If a schema name was given, the message body will be avro serialized.
     * Wait for an event to arrive before continuing (blocking)
     *
     * @param KafkaProducerMessageInterface $message
     * @return void
     */
    public function syncProduce(KafkaProducerMessageInterface $message): void
    {
        $this->produce($message, true, -1);
    }

    /**
     * Poll for producer event, pass 0 for non-blocking, pass -1 to block until an event arrives
     *
     * @param integer $timeoutMs
     * @return void
     */
    public function poll(int $timeoutMs = 0): void
    {
        $this->producer->poll($timeoutMs);
    }

    /**
     * Poll for producer events until the number of $queueSize events remain
     *
     * @param integer $timeoutMs
     * @param integer $queueSize
     * @return void
     */
    public function pollUntilQueueSizeReached(int $timeoutMs = 0, int $queueSize = 0): void
    {
        while ($this->producer->getOutQLen() > $queueSize) {
            $this->producer->poll($timeoutMs);
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
     * @param integer $timeoutMs
     * @return integer
     */
    public function flush(int $timeoutMs): int
    {
        return $this->producer->flush($timeoutMs);
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
        $topic = $this->producer->newTopic($topicName);
        return $this->producer
            ->getMetadata(
                false,
                $topic,
                $timeoutMs
            )
            ->getTopics()
            ->current();
    }

    /**
     * Start a producer transaction
     *
     * @param int $timeoutMs
     * @return void
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function beginTransaction(int $timeoutMs): void
    {
        try {
            if (false === $this->transactionInitialized) {
                $this->producer->initTransactions($timeoutMs);
                $this->transactionInitialized = true;
            }

            $this->producer->beginTransaction();
        } catch (RdKafkaErrorException $e) {
            $this->handleTransactionError($e);
        }
    }

    /**
     * Commit the current producer transaction
     *
     * @param int $timeoutMs
     * @return void
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function commitTransaction(int $timeoutMs): void
    {
        try {
            $this->producer->commitTransaction($timeoutMs);
        } catch (RdKafkaErrorException $e) {
            $this->handleTransactionError($e);
        }
    }

    /**
     * Abort the current producer transaction
     *
     * @param int $timeoutMs
     * @return void
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    public function abortTransaction(int $timeoutMs): void
    {
        try {
            $this->producer->abortTransaction($timeoutMs);
        } catch (RdKafkaErrorException $e) {
            $this->handleTransactionError($e);
        }
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

    /**
     * @param RdKafkaErrorException $e
     *
     * @throws KafkaProducerTransactionAbortException
     * @throws KafkaProducerTransactionFatalException
     * @throws KafkaProducerTransactionRetryException
     */
    private function handleTransactionError(RdKafkaErrorException $e): void
    {
        if (true === $e->isRetriable()) {
            throw new KafkaProducerTransactionRetryException(
                sprintf(
                    KafkaProducerTransactionRetryException::RETRIABLE_TRANSACTION_EXCEPTION_MESSAGE,
                    $e->getMessage()
                ),
                $e->getCode(),
                $e
            );
        } elseif (true === $e->transactionRequiresAbort()) {
            throw new KafkaProducerTransactionAbortException(
                sprintf(
                    KafkaProducerTransactionAbortException::TRANSACTION_REQUIRES_ABORT_EXCEPTION_MESSAGE,
                    $e->getMessage()
                ),
                $e->getCode(),
                $e
            );
        } else {
            $this->transactionInitialized = false;
            // according to librdkafka documentation, everything that is not retriable, abortable or fatal is fatal
            // fatal errors (so stated), need the producer to be destroyed
            throw new KafkaProducerTransactionFatalException(
                sprintf(
                    KafkaProducerTransactionFatalException::FATAL_TRANSACTION_EXCEPTION_MESSAGE,
                    $e->getMessage()
                ),
                $e->getCode(),
                $e
            );
        }
    }
}
