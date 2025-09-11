<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

abstract class AbstractKafkaMessage implements KafkaMessageInterface
{
    /**
     * @var string|null
     */
    protected $key;

    protected mixed $body = null;

    /**
     * @var string
     */
    protected $topicName;

    /**
     * @var int
     */
    protected $partition;

    /**
     * @var array<string, int|string>|null
     */
    protected $headers;

    /**
     * @return string|null
     */
    public function getKey()
    {
        return $this->key;
    }

    public function getBody(): mixed
    {
        return $this->body;
    }

    public function getTopicName(): string
    {
        return $this->topicName;
    }

    public function getPartition(): int
    {
        return $this->partition;
    }

    /**
     * @return array<string, int|string>|null
     */
    public function getHeaders(): ?array
    {
        return $this->headers;
    }
}
