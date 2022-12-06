<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Message\Encoder\EncoderInterface;

interface KafkaProducerBuilderInterface
{
    /**
     * @return KafkaProducerInterface
     */
    public function build(): KafkaProducerInterface;

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
     * @param string[] $config
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
     * Callback for log related events
     *
     * @param callable $logCallback
     * @return KafkaProducerBuilderInterface
     */
    public function withLogCallback(callable $logCallback): self;

    /**
     * Lets you set a custom encoder for produce message
     *
     * @param EncoderInterface $encoder
     * @return KafkaProducerBuilderInterface
     */
    public function withEncoder(EncoderInterface $encoder): self;
}
