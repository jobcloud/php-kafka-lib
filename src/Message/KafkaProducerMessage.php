<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

final class KafkaProducerMessage extends AbstractKafkaMessage implements KafkaProducerMessageInterface
{
    private function __construct(string $topicName, int $partition)
    {
        $this->topicName = $topicName;
        $this->partition = $partition;
    }

    public static function create(string $topicName, int $partition): KafkaProducerMessageInterface
    {
        return new self($topicName, $partition);
    }

    public function withKey(?string $key): KafkaProducerMessageInterface
    {
        $new = clone $this;

        $new->key = $key;

        return $new;
    }

    public function withBody(mixed $body): KafkaProducerMessageInterface
    {
        $new = clone $this;

        $new->body = $body;

        return $new;
    }

    public function withHeaders(?array $headers): KafkaProducerMessageInterface
    {
        $new = clone $this;

        $new->headers = $headers;

        return $new;
    }

    /**
     * @param string|integer $value
     */
    public function withHeader(string $key, $value): KafkaProducerMessageInterface
    {
        $new = clone $this;

        $new->headers[$key] = $value;

        return $new;
    }
}
