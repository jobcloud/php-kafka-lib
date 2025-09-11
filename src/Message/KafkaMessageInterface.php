<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

interface KafkaMessageInterface
{
    /**
     * Returns the message key or null if the message doesn't have a body
     * @return mixed
     */
    public function getKey();

    public function getTopicName(): string;

    public function getPartition(): int;

    /**
     * @return string[]|null
     */
    public function getHeaders(): ?array;

    /**
     * Returns the message body or null if the message doesn't have a body
     */
    public function getBody(): mixed;
}
