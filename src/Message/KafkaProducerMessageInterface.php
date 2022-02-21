<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

interface KafkaProducerMessageInterface extends KafkaMessageInterface
{
    /**
     * @param string  $topicName
     * @param integer $partition
     * @return KafkaProducerMessageInterface
     */
    public static function create(string $topicName, int $partition): KafkaProducerMessageInterface;

    /**
     * @param string|null $key
     * @return KafkaProducerMessageInterface
     */
    public function withKey(?string $key): KafkaProducerMessageInterface;

    /**
     * @param mixed $body
     * @return KafkaProducerMessageInterface
     */
    public function withBody($body): KafkaProducerMessageInterface;

    /**
     * @param string[]|null $headers
     * @return KafkaProducerMessageInterface
     */
    public function withHeaders(?array $headers): KafkaProducerMessageInterface;

    /**
     * @param string         $key
     * @param string|integer $value
     * @return KafkaProducerMessageInterface
     */
    public function withHeader(string $key, $value): KafkaProducerMessageInterface;
}
