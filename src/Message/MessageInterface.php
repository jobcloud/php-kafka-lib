<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Message;

interface MessageInterface
{

    /**
     * Returns the message body as string or null if the message doesn't have a body
     * @return mixed
     */
    public function getBody();
}
