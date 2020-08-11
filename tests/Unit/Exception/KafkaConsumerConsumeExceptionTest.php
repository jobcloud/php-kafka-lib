<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Kafka\Exception;

use Jobcloud\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Kafka\Message\KafkaConsumerMessageInterface;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Exception\KafkaConsumerConsumeException
 */
class KafkaConsumerConsumeExceptionTest extends TestCase
{
    public function testGetAndConstructOfKafkaConsumerConsumeException()
    {
        $message = $this->getMockForAbstractClass(KafkaConsumerMessageInterface::class);

        $exception = new KafkaConsumerConsumeException('', 0, $message);

        self::assertSame($message, $exception->getKafkaMessage());
    }

    public function testGetAndConstructOfKafkaConsumerConsumeExceptionWithNullAsMessage()
    {
        $exception = new KafkaConsumerConsumeException('test', 100, null);

        self::assertNull($exception->getKafkaMessage());
        self::assertEquals('test', $exception->getMessage());
        self::assertEquals(100, $exception->getCode());
    }

    public function testGetDefaults()
    {
        $exception = new KafkaConsumerConsumeException();

        self::assertNull($exception->getKafkaMessage());
        self::assertEquals('', $exception->getMessage());
        self::assertEquals(0, $exception->getCode());
        self::assertNull($exception->getPrevious());

    }
}
