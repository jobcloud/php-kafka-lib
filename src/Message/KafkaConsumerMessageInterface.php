<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

interface KafkaConsumerMessageInterface extends KafkaMessageInterface
{
    public function getOffset(): int;

    public function getTimestamp(): int;
}
