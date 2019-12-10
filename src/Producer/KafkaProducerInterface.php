<?php

namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;

interface KafkaProducerInterface
{

    /**
     * @param KafkaProducerMessageInterface $message
     * @return void
     */
    public function produce(KafkaProducerMessageInterface $message): void;

    /**
     * Purge producer messages that are in flight
     *
     * @param integer $purgeFlags
     * @return integer
     */
    public function purge(int $purgeFlags): int;

    /**
     * Wait until all outstanding produce requests are completed
     *
     * @param integer $timeout
     * @return integer
     */
    public function flush(int $timeout): int;
}
