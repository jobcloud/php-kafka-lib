<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

interface KafkaProducerMessageInterface extends KafkaMessageInterface
{
    public static function create(string $topicName, int $partition): KafkaProducerMessageInterface;

    public function withKey(?string $key): KafkaProducerMessageInterface;

    /**
     * @param mixed $body
     */
    public function withBody($body): KafkaProducerMessageInterface;

    /**
     * @param string[]|null $headers
     */
    public function withHeaders(?array $headers): KafkaProducerMessageInterface;

    /**
     * @param string|integer $value
     */
    public function withHeader(string $key, $value): KafkaProducerMessageInterface;
}
