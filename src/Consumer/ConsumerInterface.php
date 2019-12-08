<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Consumer;

use Jobcloud\Messaging\Message\MessageInterface;

interface ConsumerInterface
{
    /**
     * Consumes a message and returns it
     * @return MessageInterface The consumed message
     * @throws ConsumerException If no more messages or an error occurs
     */
    public function consume(): MessageInterface;

    /**
     * @param MessageInterface[]|MessageInterface $messages
     * @return void
     */
    public function commit($messages): void;
}
