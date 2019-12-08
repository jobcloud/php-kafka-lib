<?php

namespace Jobcloud\Messaging\Tests\Unit\Producer;

use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Producer\ProducerInterface;
use Jobcloud\Messaging\Producer\ProducerPool;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class ProducerPoolTest extends TestCase
{

    /** @var ProducerInterface|MockObject */
    private $kafkaProducerMock;

    /** @var $producerPool ProducerPool */
    private $producerPool;

    /**
     * @return void
     */
    public function setUp(): void
    {
        $this->kafkaProducerMock = $this->createMock(ProducerInterface::class);
        $this->producerPool = new ProducerPool();
    }

    /**
     * @
     */
    public function testAddProducer(): void
    {
        $this->producerPool->addProducer($this->kafkaProducerMock);
        $producers = $this->producerPool->getProducerPool();

        self::assertIsArray($producers);
        self::assertNotEmpty($producers);
        self::assertTrue(1 == count($this->producerPool->getProducerPool()));
    }

    /**
     * @return void
     */
    public function testProduce(): void
    {
        $message = $this->createMock(MessageInterface::class);

        $this->kafkaProducerMock
            ->expects(self::once())
            ->method('produce')
            ->with($message);
        $this->producerPool->addProducer($this->kafkaProducerMock);
        $this->producerPool->produce($message);
    }
}
