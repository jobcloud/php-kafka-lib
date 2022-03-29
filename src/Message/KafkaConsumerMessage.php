<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

final class KafkaConsumerMessage extends AbstractKafkaMessage implements KafkaConsumerMessageInterface
{
    /**
     * @var int
     */
    private $offset;

    /**
     * @var int
     */
    private $timestamp;


    /**
     * @param string      $topicName
     * @param integer     $partition
     * @param integer     $offset
     * @param integer     $timestamp
     * @param mixed       $key
     * @param mixed       $body
     * @param string[]|null  $headers
     */
    public function __construct(
        string $topicName,
        int $partition,
        int $offset,
        int $timestamp,
        $key,
        $body,
        ?array $headers
    ) {
        $this->topicName = $topicName;
        $this->partition = $partition;
        $this->offset = $offset;
        $this->timestamp = $timestamp;
        $this->key = $key;
        $this->body = $body;
        $this->headers = $headers;
    }

    /**
     * @return integer
     */
    public function getOffset(): int
    {
        return $this->offset;
    }

    /**
     * @return integer
     */
    public function getTimestamp(): int
    {
        return $this->timestamp;
    }
}
