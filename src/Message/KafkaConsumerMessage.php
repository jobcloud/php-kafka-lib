<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

final class KafkaConsumerMessage extends AbstractKafkaMessage implements KafkaConsumerMessageInterface
{
    /**
     * @param mixed         $key
     * @param mixed         $body
     * @param string[]|null $headers
     */
    public function __construct(
        string $topicName,
        int $partition,
        private int $offset,
        private int $timestamp,
        $key,
        $body,
        ?array $headers
    ) {
        $this->topicName = $topicName;
        $this->partition = $partition;
        $this->key = $key;
        $this->body = $body;
        $this->headers = $headers;
    }

    public function getOffset(): int
    {
        return $this->offset;
    }

    public function getTimestamp(): int
    {
        return $this->timestamp;
    }
}
