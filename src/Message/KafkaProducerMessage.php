<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

final class KafkaProducerMessage extends AbstractKafkaMessage implements KafkaProducerMessageInterface
{
    /**
     * @param string  $topicName
     * @param integer $partition
     */
    private function __construct(string $topicName, int $partition)
    {
        $this->topicName    = $topicName;
        $this->partition    = $partition;
    }

    /**
     * @param string  $topicName
     * @param integer $partition
     * @return KafkaProducerMessageInterface
     */
    public static function create(string $topicName, int $partition): KafkaProducerMessageInterface
    {
        return new self($topicName, $partition);
    }

    /**
     * @param string|null $key
     * @return KafkaProducerMessageInterface
     */
    public function withKey(?string $key): KafkaProducerMessageInterface
    {
        $new = clone $this;

        $new->key = $key;

        return $new;
    }

    /**
     * @param mixed $body
     * @return KafkaProducerMessageInterface
     */
    public function withBody($body): KafkaProducerMessageInterface
    {
        $new = clone $this;

        $new->body = $body;

        return $new;
    }

    /**
     * @param string[]|null $headers
     * @return KafkaProducerMessageInterface
     */
    public function withHeaders(?array $headers): KafkaProducerMessageInterface
    {
        $new = clone $this;

        $new->headers = $headers;

        return $new;
    }

    /**
     * @param string         $key
     * @param string|integer $value
     * @return KafkaProducerMessageInterface
     */
    public function withHeader(string $key, $value): KafkaProducerMessageInterface
    {
        $new = clone $this;

        $new->headers[$key] = $value;

        return $new;
    }
}
