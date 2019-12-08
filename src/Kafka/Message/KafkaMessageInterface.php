<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

use Jobcloud\Messaging\Message\MessageInterface;

interface KafkaMessageInterface extends MessageInterface
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
     * @return array|null
     */
    public function getHeaders(): ?array;
}
