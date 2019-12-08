<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Producer;

use Jobcloud\Messaging\Message\MessageInterface;

interface ProducerInterface
{

    /**
     * @param MessageInterface $message
     * @return void
     */
    public function produce(MessageInterface $message): void;
}
