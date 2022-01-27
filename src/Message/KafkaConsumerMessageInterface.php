<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Message;

interface KafkaConsumerMessageInterface extends KafkaMessageInterface
{
    /**
     * @return integer
     */
    public function getOffset(): int;

    /**
     * @return integer
     */
    public function getTimestamp(): int;
}
