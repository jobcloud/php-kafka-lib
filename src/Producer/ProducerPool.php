<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Producer;

use Jobcloud\Messaging\Message\MessageInterface;

final class ProducerPool implements ProducerInterface
{

    /**
     * @var ProducerInterface[]
     */
    private $producers;

    /**
     * @param ProducerInterface[] $producers
     */
    public function __construct(array $producers = [])
    {
        $this->producers = $producers;
    }

    /**
     * @param MessageInterface $message
     * @return void
     */
    public function produce(MessageInterface $message): void
    {
        foreach ($this->producers as $producer) {
            $producer->produce($message);
        }
    }

    /**
     * @return array
     */
    public function getProducerPool(): array
    {
        return $this->producers;
    }

    /**
     * @param ProducerInterface $producer
     * @return ProducerPool
     */
    public function addProducer(ProducerInterface $producer): self
    {
        $this->producers[] = $producer;

        return $this;
    }
}
