<?php

declare(strict_types=1);

namespace Jobcloud\Kafka\Tests\Unit\Message\Encoder;

use Jobcloud\Kafka\Message\Encoder\JsonEncoder;
use Jobcloud\Kafka\Message\KafkaProducerMessageInterface;
use JsonException;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Kafka\Message\Encoder\JsonEncoder
 */
class JsonEncoderTest extends TestCase
{
    public function testEncode(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(['name' => 'foo']);
        $message->expects(self::once())->method('withBody')->with('{"name":"foo"}')->willReturn($message);

        $encoder = new JsonEncoder();

        self::assertSame($message, $encoder->encode($message));
    }

    public function testEncodeThrowsException(): void
    {
        $message = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $message->expects(self::once())->method('getBody')->willReturn(chr(255));

        $encoder = new JsonEncoder();

        $this->expectException(JsonException::class);

        $encoder->encode($message);
    }
}
