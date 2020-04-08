<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

interface KafkaMessageInterface
{

    /**
     * @return string|null
     */
    public function getKey(): ?string;

    /**
     * @return string
     */
    public function getTopicName(): string;

    /**
     * @return integer
     */
    public function getPartition(): int;

    /**
     * @return string[]|null
     */
    public function getHeaders(): ?array;

    /**
     * Returns the message body as string or null if the message doesn't have a body
     * @return mixed
     */
    public function getBody();
}
