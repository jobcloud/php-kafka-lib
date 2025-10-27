<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Message\Encoder\EncoderInterface;

interface KafkaProducerBuilderInterface
{
    public function build(): KafkaProducerInterface;

    public static function create(): self;

    public function withAdditionalBroker(string $broker): self;

    /**
     * @param string[] $config
     */
    public function withAdditionalConfig(array $config): self;

    public function withDeliveryReportCallback(callable $deliveryReportCallback): self;

    public function withErrorCallback(callable $errorCallback): self;

    /**
     * Callback for log related events
     */
    public function withLogCallback(callable $logCallback): self;

    /**
     * Lets you set a custom encoder for produce message
     */
    public function withEncoder(EncoderInterface $encoder): self;
}
