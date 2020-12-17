<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Kafka\Callback\KafkaProducerDeliveryReportCallback;
use Jobcloud\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Kafka\Exception\KafkaProducerException;
use Jobcloud\Kafka\Message\Encoder\EncoderInterface;
use Jobcloud\Kafka\Message\Encoder\NullEncoder;
use RdKafka\Producer as RdKafkaProducer;

final class KafkaProducerBuilder implements KafkaProducerBuilderInterface
{
    /**
     * @var array|string[]
     */
    private $brokers = [];

    /**
     * @var string[]
     */
    private $config = [];

    /**
     * @var callable
     */
    private $deliverReportCallback;

    /**
     * @var callable
     */
    private $errorCallback;

    /**
     * @var callable
     */
    private $logCallback;

    /**
     * @var EncoderInterface
     */
    private $encoder;

    /**
     * KafkaProducerBuilder constructor.
     */
    private function __construct()
    {
        $this->deliverReportCallback = new KafkaProducerDeliveryReportCallback();
        $this->errorCallback = new KafkaErrorCallback();
        $this->encoder = new NullEncoder();
    }

    /**
     * Returns the producer builder
     *
     * @return KafkaProducerBuilderInterface
     */
    public static function create(): KafkaProducerBuilderInterface
    {
        return new self();
    }

    /**
     * Adds a broker to which you want to produce
     *
     * @param string $broker
     * @return KafkaProducerBuilderInterface
     */
    public function withAdditionalBroker(string $broker): KafkaProducerBuilderInterface
    {
        $this->brokers[] = $broker;

        return $this;
    }

    /**
     * Add configuration settings, otherwise the kafka defaults apply
     *
     * @param string[] $config
     * @return KafkaProducerBuilderInterface
     */
    public function withAdditionalConfig(array $config): KafkaProducerBuilderInterface
    {
        $this->config = $config + $this->config;

        return $this;
    }

    /**
     * Sets callback for the delivery report. The broker will send a delivery
     * report for every message which describes if the delivery was successful or not
     *
     * @param callable $deliveryReportCallback
     * @return KafkaProducerBuilderInterface
     */
    public function withDeliveryReportCallback(callable $deliveryReportCallback): KafkaProducerBuilderInterface
    {
        $this->deliverReportCallback = $deliveryReportCallback;

        return $this;
    }

    /**
     * Set a callback to be called on errors.
     * The default callback will throw an exception for every error
     *
     * @param callable $errorCallback
     * @return KafkaProducerBuilderInterface
     */
    public function withErrorCallback(callable $errorCallback): KafkaProducerBuilderInterface
    {
        $this->errorCallback = $errorCallback;

        return $this;
    }

    /**
     * Callback for log related events
     *
     * @param callable $logCallback
     * @return KafkaProducerBuilderInterface
     */
    public function withLogCallback(callable $logCallback): KafkaProducerBuilderInterface
    {
        $this->logCallback = $logCallback;

        return $this;
    }

    /**
     * Lets you set a custom encoder for produce message
     *
     * @param EncoderInterface $encoder
     * @return KafkaProducerBuilderInterface
     */
    public function withEncoder(EncoderInterface $encoder): KafkaProducerBuilderInterface
    {
        $this->encoder = $encoder;

        return $this;
    }

    /**
     * Returns your producer instance
     *
     * @return KafkaProducerInterface
     * @throws KafkaProducerException
     */
    public function build(): KafkaProducerInterface
    {
        if ([] === $this->brokers) {
            throw new KafkaProducerException(KafkaProducerException::NO_BROKER_EXCEPTION_MESSAGE);
        }

        //Thread termination improvement (https://github.com/arnaud-lb/php-rdkafka#performance--low-latency-settings)
        $this->config['socket.timeout.ms'] = '50';
        $this->config['queue.buffering.max.ms'] = '1';

        if (function_exists('pcntl_sigprocmask')) {
            pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
            $this->config['internal.termination.signal'] = (string) SIGIO;
            unset($this->config['queue.buffering.max.ms']);
        }

        $kafkaConfig = new KafkaConfiguration($this->brokers, [], $this->config);

        //set producer callbacks
        $this->registerCallbacks($kafkaConfig);

        $rdKafkaProducer = new RdKafkaProducer($kafkaConfig);

        return new KafkaProducer($rdKafkaProducer, $kafkaConfig, $this->encoder);
    }

    /**
     * @param KafkaConfiguration $conf
     * @return void
     */
    private function registerCallbacks(KafkaConfiguration $conf): void
    {
        $conf->setDrMsgCb($this->deliverReportCallback);
        $conf->setErrorCb($this->errorCallback);

        if (null !== $this->logCallback) {
            $conf->setLogCb($this->logCallback);
        }
    }
}
