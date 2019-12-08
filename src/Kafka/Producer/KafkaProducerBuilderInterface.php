<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Message\Encoder\EncoderInterface;
use Jobcloud\Messaging\Producer\ProducerInterface;

interface KafkaProducerBuilderInterface
{
    /**
     * @return ProducerInterface
     */
    public function build(): ProducerInterface;

    /**
     * @return KafkaProducerBuilderInterface
     */
    public static function create(): self;

    /**
     * @param string $broker
     * @return KafkaProducerBuilderInterface
     */
    public function withAdditionalBroker(string $broker): self;

    /**
     * @param array $config
     * @return KafkaProducerBuilderInterface
     */
    public function withAdditionalConfig(array $config): self;

    /**
     * @param callable $deliveryReportCallback
     * @return KafkaProducerBuilderInterface
     */
    public function withDeliveryReportCallback(callable $deliveryReportCallback): self;

    /**
     * @param callable $errorCallback
     * @return KafkaProducerBuilderInterface
     */
    public function withErrorCallback(callable $errorCallback): self;

    /**
     * @param integer $pollTimeout
     * @return KafkaProducerBuilderInterface
     */
    public function withPollTimeout(int $pollTimeout): self;

    /**
     * Lets you set a custom encoder for produce message
     *
     * @param EncoderInterface $encoder
     * @return KafkaProducerBuilderInterface
     */
    public function withEncoder(EncoderInterface $encoder): self;
}
