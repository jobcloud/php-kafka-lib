<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Consumer;

use Jobcloud\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Kafka\Exception\KafkaConsumerBuilderException;
use Jobcloud\Kafka\Message\Decoder\DecoderInterface;
use Jobcloud\Kafka\Message\Decoder\NullDecoder;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;

final class KafkaConsumerBuilder implements KafkaConsumerBuilderInterface
{
    public const CONSUMER_TYPE_LOW_LEVEL = 'low';
    public const CONSUMER_TYPE_HIGH_LEVEL = 'high';

    /**
     * @var string[]
     */
    private $brokers = [];

    /**
     * @var array<string, mixed>
     */
    private $config = [
        'enable.auto.offset.store' => false,
        'enable.auto.commit' => false,
        'auto.offset.reset' => 'earliest'
    ];

    /**
     * @var array|TopicSubscription[]
     */
    private $topics = [];

    /**
     * @var string
     */
    private $consumerGroup = 'default';

    /**
     * @var string
     */
    private $consumerType = self::CONSUMER_TYPE_HIGH_LEVEL;

    /**
     * @var callable
     */
    private $errorCallback;

    /**
     * @var callable
     */
    private $rebalanceCallback;

    /**
     * @var callable
     */
    private $consumeCallback;

    /**
     * @var callable
     */
    private $logCallback;

    /**
     * @var callable
     */
    private $offsetCommitCallback;

    /**
     * @var DecoderInterface
     */
    private $decoder;

    /**
     * KafkaConsumerBuilder constructor.
     */
    private function __construct()
    {
        $this->errorCallback = new KafkaErrorCallback();
        $this->decoder = new NullDecoder();
    }

    /**
     * Returns the builder
     *
     * @return KafkaConsumerBuilder
     */
    public static function create(): self
    {
        return new self();
    }

    /**
     * Adds a broker from which you want to consume
     *
     * @param string $broker
     * @return KafkaConsumerBuilderInterface
     */
    public function withAdditionalBroker(string $broker): KafkaConsumerBuilderInterface
    {
        $that = clone $this;

        $that->brokers[] = $broker;

        return $that;
    }

    /**
     * Add topic name(s) (and additionally partitions and offsets) to subscribe to
     *
     * @param string  $topicName
     * @param int[]   $partitions
     * @param integer $offset
     * @return KafkaConsumerBuilderInterface
     */
    public function withAdditionalSubscription(
        string $topicName,
        array $partitions = [],
        int $offset = self::OFFSET_STORED
    ): KafkaConsumerBuilderInterface {
        $that = clone $this;

        $that->topics[] = new TopicSubscription($topicName, $partitions, $offset);

        return $that;
    }

    /**
     * Replaces all topic names previously configured with a topic and additionally partitions and an offset to
     * subscribe to
     *
     * @param string  $topicName
     * @param int[]   $partitions
     * @param integer $offset
     * @return KafkaConsumerBuilderInterface
     */
    public function withSubscription(
        string $topicName,
        array $partitions = [],
        int $offset = self::OFFSET_STORED
    ): KafkaConsumerBuilderInterface {
        $that = clone $this;

        $that->topics = [new TopicSubscription($topicName, $partitions, $offset)];

        return $that;
    }

    /**
     * Add configuration settings, otherwise the kafka defaults apply
     *
     * @param string[] $config
     * @return KafkaConsumerBuilderInterface
     */
    public function withAdditionalConfig(array $config): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->config = $config + $this->config;

        return $that;
    }

    /**
     * Set the consumer group
     *
     * @param string $consumerGroup
     * @return KafkaConsumerBuilderInterface
     */
    public function withConsumerGroup(string $consumerGroup): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->consumerGroup = $consumerGroup;

        return $that;
    }

    /**
     * Set the consumer type, can be either CONSUMER_TYPE_LOW_LEVEL or CONSUMER_TYPE_HIGH_LEVEL
     *
     * @param string $consumerType
     * @return KafkaConsumerBuilderInterface
     */
    public function withConsumerType(string $consumerType): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->consumerType = $consumerType;

        return $that;
    }

    /**
     * Set a callback to be called on errors.
     * The default callback will throw an exception for every error
     *
     * @param callable $errorCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withErrorCallback(callable $errorCallback): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->errorCallback = $errorCallback;

        return $that;
    }

    /**
     * Set a callback to be called on consumer rebalance
     *
     * @param callable $rebalanceCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withRebalanceCallback(callable $rebalanceCallback): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->rebalanceCallback = $rebalanceCallback;

        return $that;
    }

    /**
     * Only applicable for the high level consumer
     * Callback that is going to be called when you call consume
     *
     * @param callable $consumeCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withConsumeCallback(callable $consumeCallback): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->consumeCallback = $consumeCallback;

        return $that;
    }

    /**
     * Callback for log related events
     *
     * @param callable $logCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withLogCallback(callable $logCallback): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->logCallback = $logCallback;

        return $that;
    }

    /**
     * Set callback that is being called on offset commits
     *
     * @param callable $offsetCommitCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function withOffsetCommitCallback(callable $offsetCommitCallback): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->offsetCommitCallback = $offsetCommitCallback;

        return $that;
    }

    /**
     * Lets you set a custom decoder for the consumed message
     *
     * @param DecoderInterface $decoder
     * @return KafkaConsumerBuilderInterface
     */
    public function withDecoder(DecoderInterface $decoder): KafkaConsumerBuilderInterface
    {
        $that = clone $this;
        $that->decoder = $decoder;

        return $that;
    }

    /**
     * Returns your consumer instance
     *
     * @return KafkaConsumerInterface
     * @throws KafkaConsumerBuilderException
     */
    public function build(): KafkaConsumerInterface
    {
        if ([] === $this->brokers) {
            throw new KafkaConsumerBuilderException(KafkaConsumerBuilderException::NO_BROKER_EXCEPTION_MESSAGE);
        }

        //set additional config
        $this->config['group.id'] = $this->consumerGroup;

        //create config
        $kafkaConfig = new KafkaConfiguration(
            $this->brokers,
            $this->topics,
            $this->config,
            $this->consumerType
        );

        //set consumer callbacks
        $this->registerCallbacks($kafkaConfig);

        //create RdConsumer
        if (self::CONSUMER_TYPE_LOW_LEVEL === $this->consumerType) {
            if (null !== $this->consumeCallback) {
                throw new KafkaConsumerBuilderException(
                    sprintf(
                        KafkaConsumerBuilderException::UNSUPPORTED_CALLBACK_EXCEPTION_MESSAGE,
                        'consumerCallback',
                        KafkaLowLevelConsumer::class
                    )
                );
            }

            $rdKafkaConsumer = new RdKafkaLowLevelConsumer($kafkaConfig);

            return new KafkaLowLevelConsumer(
                $rdKafkaConsumer,
                $kafkaConfig,
                $this->decoder
            );
        }

        $rdKafkaConsumer = new RdKafkaHighLevelConsumer($kafkaConfig);

        return new KafkaHighLevelConsumer($rdKafkaConsumer, $kafkaConfig, $this->decoder);
    }

    /**
     * @param KafkaConfiguration $conf
     * @return void
     */
    private function registerCallbacks(KafkaConfiguration $conf): void
    {
        $conf->setErrorCb($this->errorCallback);

        if (null !== $this->rebalanceCallback) {
            $conf->setRebalanceCb($this->rebalanceCallback);
        }

        if (null !== $this->consumeCallback) {
            $conf->setConsumeCb($this->consumeCallback);
        }

        if (null !== $this->logCallback) {
            $conf->setLogCb($this->logCallback);
        }

        if (null !== $this->offsetCommitCallback) {
            $conf->setOffsetCommitCb($this->offsetCommitCallback);
        }
    }
}
