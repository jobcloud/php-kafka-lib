<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

abstract class AbstractKafkaMessage implements KafkaMessageInterface
{
    /**
     * @var string|null
     */
    protected $key;

    /**
     * @var mixed
     */
    protected $body;

    /**
     * @var string
     */
    protected $topicName;

    /**
     * @var int
     */
    protected $partition;

    /**
     * @var string[]|null
     */
    protected $headers;

    /**
     * @return mixed
     */
    public function getKey()
    {
        return $this->key;
    }

    /**
     * @return mixed
     */
    public function getBody()
    {
        return $this->body;
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @return integer
     */
    public function getPartition(): int
    {
        return $this->partition;
    }

    /**
     * @return string[]|null
     */
    public function getHeaders(): ?array
    {
        return $this->headers;
    }
}
